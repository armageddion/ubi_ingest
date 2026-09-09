import sqlite3
import threading
import os
from datetime import datetime, timezone


class StateStore:
    def __init__(self, path):
        self.path = path
        self._lock = threading.RLock()
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS customer_runs (
                customer_id TEXT PRIMARY KEY,
                last_run_at TEXT,
                last_run_status TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS product_hashes (
                customer_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                PRIMARY KEY (customer_id, product_id)
            );
            """
        )
        self._connection.commit()

    def get_last_run(self, customer_id):
        with self._lock:
            row = self._connection.execute(
                "SELECT last_run_at, last_run_status FROM customer_runs WHERE customer_id = ?",
                (customer_id,),
            ).fetchone()
        if row is None:
            return None
        return {"last_run_at": row[0], "last_run_status": row[1]}

    def set_last_run(self, customer_id, run_at, status):
        timestamp = run_at.astimezone(timezone.utc).isoformat()
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO customer_runs(customer_id, last_run_at, last_run_status)
                VALUES (?, ?, ?)
                ON CONFLICT(customer_id) DO UPDATE SET
                    last_run_at = excluded.last_run_at,
                    last_run_status = excluded.last_run_status
                """,
                (customer_id, timestamp, status),
            )
            self._connection.commit()

    def get_hashes(self, customer_id):
        with self._lock:
            rows = self._connection.execute(
                "SELECT product_id, content_hash FROM product_hashes WHERE customer_id = ?",
                (customer_id,),
            ).fetchall()
        return dict(rows)

    def update_hashes(self, customer_id, hashes):
        if not hashes:
            return
        with self._lock:
            self._connection.executemany(
                """
                INSERT INTO product_hashes(customer_id, product_id, content_hash)
                VALUES (?, ?, ?)
                ON CONFLICT(customer_id, product_id) DO UPDATE SET
                    content_hash = excluded.content_hash
                """,
                [(customer_id, product_id, content_hash) for product_id, content_hash in hashes.items()],
            )
            self._connection.commit()

    def remove_hashes(self, customer_id, product_ids):
        if not product_ids:
            return
        with self._lock:
            self._connection.executemany(
                "DELETE FROM product_hashes WHERE customer_id = ? AND product_id = ?",
                [(customer_id, product_id) for product_id in product_ids],
            )
            self._connection.commit()

    def close(self):
        with self._lock:
            self._connection.close()


def utc_now():
    return datetime.now(timezone.utc)
