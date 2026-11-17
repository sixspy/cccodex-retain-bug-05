import sqlite3

from app.services.locking import begin_immediate, write_lock


def _collect_snapshot(conn: sqlite3.Connection) -> dict:
    """在同一事务、同一时点读取全部计数，保证报告数字与实物分区同源。"""
    state_rows = conn.execute(
        "SELECT storage_state, COUNT(*) AS c FROM partitions GROUP BY storage_state"
    ).fetchall()
    state_counts = {r["storage_state"]: int(r["c"]) for r in state_rows}

    total_partitions = int(
        conn.execute("SELECT COUNT(*) AS c FROM partitions").fetchone()["c"]
    )
    pending = int(
        conn.execute(
            "SELECT COUNT(*) AS c FROM judgments "
            "WHERE status IN ('待执行','执行中')"
        ).fetchone()["c"]
    )
    running = conn.execute(
        "SELECT id, partition_id FROM judgments WHERE status = '执行中'"
    ).fetchall()
    in_flight = conn.execute(
        "SELECT id, domain_code, biz_date FROM partitions "
        "WHERE storage_state = '裁撤中'"
    ).fetchall()

    return {
        "state_counts": state_counts,
        "total_partitions": total_partitions,
        "pending": pending,
        "running": [dict(r) for r in running],
        "in_flight": [dict(r) for r in in_flight],
    }


def run_reconcile(conn: sqlite3.Connection, as_of_date: str) -> dict:
    """策略视角与实物分区对账。

    落库形态（全局唯一，无开关）：**提交瞬间按当前库重算的物化摘要**。
    计数全部来自同一写事务、同一时点的实读，报告与实物同源；发现混数
    （裁撤中分区 / 执行中裁决 / 计数勾稽不上）直接拦截，不写库。
    """
    # 两名回放必须排队锁写，否则正式轨会被并发混数覆盖、账实割裂
    with write_lock:
        # 立即拿文件写锁：事务内所有读数与写入是同一时点快照
        begin_immediate(conn)

        snap = _collect_snapshot(conn)
        state_counts = snap["state_counts"]
        online = state_counts.get("在线", 0)
        archived = state_counts.get("已归档", 0)
        destroyed = state_counts.get("已销毁", 0)

        # —— 同源判定：混数即拦，不生成报告 ——
        if snap["in_flight"]:
            ids = "、".join(
                f"分区{p['id']}({p['domain_code']}/{p['biz_date']})"
                for p in snap["in_flight"]
            )
            raise ValueError(
                f"存在裁撤中分区（{ids}），在线/归档/销毁计数未覆盖全部实物，"
                "报告与实物不同源，已拦截本次对账；请待该裁决执行落定后再生成。"
            )
        if snap["running"]:
            ids = "、".join(f"裁决{j['id']}" for j in snap["running"])
            raise ValueError(
                f"存在执行中裁决（{ids}），正式轨尚未落定，"
                "报告与实物不同源，已拦截本次对账；请待执行结束后再生成。"
            )
        # 勾稽校验：三类存储态计数之和必须等于实物分区总数
        if online + archived + destroyed != snap["total_partitions"]:
            raise ValueError(
                "对账计数与实物分区总数勾稽不符（在线+已归档+已销毁≠分区总数），"
                "存在不同源混数，已拦截本次对账。"
            )

        mismatches = conn.execute(
            """
            SELECT j.id AS judgment_id, j.decided_action, j.status AS judgment_status,
                   p.id AS partition_id, p.storage_state
            FROM judgments j
            JOIN partitions p ON p.id = j.partition_id
            WHERE j.status = '已完成'
              AND (
                (j.decided_action = '归档' AND p.storage_state != '已归档')
                OR (j.decided_action = '销毁' AND p.storage_state != '已销毁')
              )
            """
        ).fetchall()
        mismatch_count = len(mismatches)
        pending = snap["pending"]

        # 物化摘要：所有数字此刻一次性算定并随报告落库，之后不再随库漂移
        summary = (
            f"在线{online} / 已归档{archived} / 已销毁{destroyed} / "
            f"待执行裁决{pending} / 错位{mismatch_count}"
        )
        cur = conn.execute(
            "INSERT INTO reconcile_reports "
            "(as_of_date, online_count, archived_count, destroyed_count, "
            "pending_judgment_count, mismatch_count, summary) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                as_of_date,
                online,
                archived,
                destroyed,
                pending,
                mismatch_count,
                summary,
            ),
        )
        report_id = int(cur.lastrowid)

        # 错位来源同步落库，历史报告在对账列表里仍可逐条追溯
        for m in mismatches:
            conn.execute(
                "INSERT INTO reconcile_mismatches "
                "(report_id, judgment_id, partition_id, decided_action, "
                "judgment_status, storage_state) VALUES (?,?,?,?,?,?)",
                (
                    report_id,
                    m["judgment_id"],
                    m["partition_id"],
                    m["decided_action"],
                    m["judgment_status"],
                    m["storage_state"],
                ),
            )

        report = _load_report(conn, report_id)
        return report


def _load_report(conn: sqlite3.Connection, report_id: int) -> dict:
    row = conn.execute(
        "SELECT id, as_of_date, online_count, archived_count, destroyed_count, "
        "pending_judgment_count, mismatch_count, summary, created_at "
        "FROM reconcile_reports WHERE id = ?",
        (report_id,),
    ).fetchone()
    report = dict(row)
    report["mismatches"] = [
        dict(r)
        for r in conn.execute(
            "SELECT id, judgment_id, partition_id, decided_action, "
            "judgment_status, storage_state FROM reconcile_mismatches "
            "WHERE report_id = ? ORDER BY id",
            (report_id,),
        ).fetchall()
    ]
    return report


def list_reports(conn: sqlite3.Connection) -> list[dict]:
    """对账列表：每条报告带已落库的错位来源，不依赖当次内存结果。"""
    rows = conn.execute(
        "SELECT id, as_of_date, online_count, archived_count, destroyed_count, "
        "pending_judgment_count, mismatch_count, summary, created_at "
        "FROM reconcile_reports ORDER BY id DESC"
    ).fetchall()
    reports = [dict(r) for r in rows]
    for report in reports:
        report["mismatches"] = [
            dict(r)
            for r in conn.execute(
                "SELECT id, judgment_id, partition_id, decided_action, "
                "judgment_status, storage_state FROM reconcile_mismatches "
                "WHERE report_id = ? ORDER BY id",
                (report["id"],),
            ).fetchall()
        ]
    return reports
