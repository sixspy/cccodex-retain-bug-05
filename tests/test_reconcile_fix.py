"""对账修复验收：同源计数、错位来源落库、混数拦截、回放排队锁写、本机库可验。"""

import os
import sqlite3
import threading

import pytest

from app.db import connect, init_db
from app.services import execute_judgment, run_expire_scan, run_reconcile
from app.services.locking import write_lock


def _make_db(tmp_path):
    db = tmp_path / "retain.db"
    init_db(str(db))
    return str(db)


def _scan_and_open_judgments(db):
    with connect(db) as conn:
        run_expire_scan(conn, "2026-11-16")
        rows = conn.execute(
            "SELECT id FROM judgments WHERE status='待执行' ORDER BY id"
        ).fetchall()
    return [r["id"] for r in rows]


def test_online_count_is_partition_table_not_inflated(tmp_path):
    """在线数只认 partitions 实物表；有待执行裁决也不得注水或漂移。"""
    db = _make_db(tmp_path)
    pending_ids = _scan_and_open_judgments(db)
    assert pending_ids

    with connect(db) as conn:
        online_before = conn.execute(
            "SELECT COUNT(*) c FROM partitions WHERE storage_state='在线'"
        ).fetchone()["c"]
        report = run_reconcile(conn, "2026-11-16")

    # 报告在线数 == 实物表在线数，不掺待执行裁决
    assert report["online_count"] == online_before
    assert report["pending_judgment_count"] == len(pending_ids)
    # 摘要与落库列同源
    assert f"在线{online_before}" in report["summary"]
    # 勾稽：三类计数之和 == 实物分区总数
    with connect(db) as conn:
        total = conn.execute("SELECT COUNT(*) c FROM partitions").fetchone()["c"]
    assert (
        report["online_count"]
        + report["archived_count"]
        + report["destroyed_count"]
        == total
    )


def test_report_counts_immutable_after_later_changes(tmp_path):
    """物化摘要落库后，后续执行不改变历史报告数字（提交瞬间重算、一次性写死）。"""
    db = _make_db(tmp_path)
    pending_ids = _scan_and_open_judgments(db)

    with connect(db) as conn:
        first = run_reconcile(conn, "2026-11-16")
    frozen = dict(first)

    # 之后再执行一条裁决，实物发生变化
    with connect(db) as conn:
        execute_judgment(conn, pending_ids[0])

    with connect(db) as conn:
        listed = [r for r in __import__(
            "app.services.reconcile", fromlist=["list_reports"]
        ).list_reports(conn) if r["id"] == frozen["id"]][0]
    assert listed["online_count"] == frozen["online_count"]
    assert listed["summary"] == frozen["summary"]


def test_mismatch_source_persisted_and_visible_in_list(tmp_path):
    """错位来源随报告落库：直接篡改实物制造错位，列表仍能逐条看到来源。"""
    db = _make_db(tmp_path)
    pending_ids = _scan_and_open_judgments(db)

    with connect(db) as conn:
        execute_judgment(conn, pending_ids[0])
        # 已完成裁决后，把分区实物改成与裁决不符，制造账实错位
        conn.execute(
            "UPDATE partitions SET storage_state='在线', archive_uri=NULL "
            "WHERE id=(SELECT partition_id FROM judgments WHERE id=?)",
            (pending_ids[0],),
        )
        report = run_reconcile(conn, "2026-11-16")

    assert report["mismatch_count"] == 1
    assert report["mismatches"][0]["judgment_id"] == pending_ids[0]

    # 全新连接读列表（模拟另一名用户/另一次打开页面），错位来源仍在
    from app.services.reconcile import list_reports

    with connect(db) as conn:
        rows = list_reports(conn)
    assert rows[0]["mismatch_count"] == 1
    src = rows[0]["mismatches"][0]
    assert src["judgment_id"] == pending_ids[0]
    assert src["partition_id"] == report["mismatches"][0]["partition_id"]
    assert src["storage_state"] == "在线"

    # 落库表可直接机验
    with sqlite3.connect(db) as raw:
        n = raw.execute(
            "SELECT COUNT(*) FROM reconcile_mismatches WHERE report_id=?",
            (report["id"],),
        ).fetchone()[0]
    assert n == 1


def test_mixed_state_reconcile_blocked_with_chinese_message(tmp_path):
    """裁撤中/执行中混数即拦：不写报告，ValueError 为中文提示。"""
    db = _make_db(tmp_path)
    pending_ids = _scan_and_open_judgments(db)

    # 手工把一条裁决与分区推进到执行中/裁撤中（模拟两名回放交叉的中间态）
    with connect(db) as conn:
        conn.execute("UPDATE judgments SET status='执行中' WHERE id=?", (pending_ids[0],))
        conn.execute(
            "UPDATE partitions SET storage_state='裁撤中' "
            "WHERE id=(SELECT partition_id FROM judgments WHERE id=?)",
            (pending_ids[0],),
        )

    with connect(db) as conn:
        with pytest.raises(ValueError) as exc:
            run_reconcile(conn, "2026-11-16")
        # 被拦截后事务回滚，无报告落库
        reports = conn.execute("SELECT COUNT(*) c FROM reconcile_reports").fetchone()["c"]
    assert reports == 0
    msg = str(exc.value)
    assert "拦截" in msg and ("裁撤中" in msg or "执行中" in msg)


def test_blocked_reconcile_api_returns_409_chinese(tmp_path, monkeypatch):
    """接口层：混数拦截回 409 与中文 detail。"""
    from fastapi.testclient import TestClient

    from app.main import app

    db = _make_db(tmp_path)
    monkeypatch.setenv("DATABASE_PATH", db)
    pending_ids = _scan_and_open_judgments(db)
    with connect(db) as conn:
        conn.execute("UPDATE judgments SET status='执行中' WHERE id=?", (pending_ids[0],))
        conn.execute(
            "UPDATE partitions SET storage_state='裁撤中' "
            "WHERE id=(SELECT partition_id FROM judgments WHERE id=?)",
            (pending_ids[0],),
        )

    with TestClient(app) as client:
        client.post("/api/login", json={"username": "admin", "password": "123456"})
        r = client.post("/api/reconcile-reports", json={"as_of_date": "2026-11-16"})
    assert r.status_code == 409
    assert "拦截" in r.json()["detail"]


def test_concurrent_reconciles_serialized_no_mixed_overwrite(tmp_path):
    """两名回放并发生成对账：写锁排队，两次都成功且各自同源，不交叉覆盖。"""
    db = _make_db(tmp_path)
    _scan_and_open_judgments(db)
    errors = []
    results = []

    def worker():
        try:
            with connect(db) as conn:
                results.append(run_reconcile(conn, "2026-11-16")["id"])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join(timeout=30)
    t2.join(timeout=30)

    assert not errors
    assert len(results) == 2 and len(set(results)) == 2
    with sqlite3.connect(db) as raw:
        n = raw.execute("SELECT COUNT(*) FROM reconcile_reports").fetchone()[0]
        # 两份报告均为完整提交，无半成品
        assert n == 2
        bad = raw.execute(
            "SELECT COUNT(*) FROM reconcile_reports "
            "WHERE online_count + archived_count + destroyed_count != "
            "(SELECT COUNT(*) FROM partitions)"
        ).fetchone()[0]
    assert bad == 0


def test_write_lock_is_single_global_lock():
    """扫描/执行/对账共用同一把锁，而非各搞一套。"""
    from app.services import execute as execute_mod
    from app.services import reconcile as reconcile_mod
    from app.services import scan as scan_mod

    assert scan_mod.write_lock is execute_mod.write_lock
    assert reconcile_mod.write_lock is write_lock


def test_judgment_api_carries_partition_state(tmp_path, monkeypatch):
    """裁决卡片接口带实物分区状态，与对账同源。"""
    from fastapi.testclient import TestClient

    from app.main import app

    db = _make_db(tmp_path)
    monkeypatch.setenv("DATABASE_PATH", db)
    with TestClient(app) as client:
        client.post("/api/login", json={"username": "admin", "password": "123456"})
        client.post("/api/scan-jobs", json={"as_of_date": "2026-11-16"})
        items = client.get("/api/judgments").json()["items"]
    assert items and all("storage_state" in j for j in items)
    assert all(j["storage_state"] == "在线" for j in items)
