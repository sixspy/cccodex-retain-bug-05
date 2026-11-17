"""全局串行写锁。

两名回放（或任意两个写请求）并发落库时，若不加锁，对账/执行/扫描的
多语句事务可能交叉，正式轨被混数覆盖、账实割裂。所有写库服务入口统一
经此锁串行化（进程内全局唯一，配合 SQLite BEGIN IMMEDIATE 兜底文件锁）。
"""

import sqlite3
import threading

write_lock = threading.RLock()


def begin_immediate(conn: sqlite3.Connection) -> bool:
    """幂等开启立即写事务。

    返回 True 表示由本次调用开启（调用方负责提交/回滚）；连接已在事务中
    则返回 False，沿用外层事务，避免 "cannot start a transaction within a
    transaction"。
    """
    if conn.in_transaction:
        return False
    conn.execute("BEGIN IMMEDIATE")
    return True
