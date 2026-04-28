"""
Enterprise YMS integration service.
Synchronizes dock assignments, trailer events, and gate activity between the YMS API and local SQLite.
"""
import json
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from yms_client import DockStatus, TrailerPosition, YMSClient, YMSConfig, YMSEvent

logger = logging.getLogger(__name__)


DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS dock_status_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dock_id TEXT NOT NULL,
    status TEXT NOT NULL,
    trailer_id TEXT,
    lane TEXT,
    capacity_tons REAL,
    recorded_at REAL,
    synced_at REAL DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS trailer_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT UNIQUE,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    received_at REAL
);

CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_type TEXT NOT NULL,
    records_written INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    started_at REAL,
    finished_at REAL
);
"""


class LocalDB:
    """SQLite persistence layer for YMS sync data."""

    def __init__(self, db_path: str = "yms_sync.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def _init_schema(self) -> None:
        conn = self._conn()
        conn.executescript(DB_SCHEMA)
        conn.commit()

    def upsert_dock_status(self, dock: DockStatus) -> None:
        conn = self._conn()
        conn.execute(
            """INSERT INTO dock_status_log (dock_id, status, trailer_id, lane, capacity_tons, recorded_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (dock.dock_id, dock.status, dock.trailer_id, dock.lane, dock.capacity_tons, dock.last_updated),
        )
        conn.commit()

    def insert_event(self, event: YMSEvent) -> bool:
        conn = self._conn()
        try:
            conn.execute(
                """INSERT OR IGNORE INTO trailer_events (event_id, event_type, payload, received_at)
                   VALUES (?, ?, ?, ?)""",
                (event.event_id, event.event_type, json.dumps(event.payload), event.received_at),
            )
            conn.commit()
            return conn.total_changes > 0
        except Exception as exc:
            logger.error("insert_event failed: %s", exc)
            return False

    def log_sync(self, sync_type: str, records: int, errors: int, started_at: float) -> None:
        conn = self._conn()
        conn.execute(
            """INSERT INTO sync_log (sync_type, records_written, errors, started_at, finished_at)
               VALUES (?, ?, ?, ?, ?)""",
            (sync_type, records, errors, started_at, time.time()),
        )
        conn.commit()

    def get_recent_dock_statuses(self, limit: int = 50) -> List[Dict]:
        rows = self._conn().execute(
            "SELECT * FROM dock_status_log ORDER BY recorded_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_event_count(self) -> int:
        return self._conn().execute("SELECT COUNT(*) FROM trailer_events").fetchone()[0]


class YMSIntegrationService:
    """
    Orchestrates periodic synchronization between the YMS API and local persistence.
    Supports full dock sync, incremental event polling, and position push.
    """

    def __init__(self, yms_client: YMSClient, local_db: LocalDB,
                 poll_interval_s: float = 30.0,
                 event_callback: Optional[Callable[[YMSEvent], None]] = None):
        self.client = yms_client
        self.db = local_db
        self.poll_interval_s = poll_interval_s
        self.event_callback = event_callback
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_event_poll: float = 0.0
        self._sync_errors = 0

    def sync_dock_statuses(self) -> int:
        """Pull dock statuses from YMS and write to local DB. Returns records written."""
        started = time.time()
        docks = self.client.get_dock_statuses()
        written = 0
        errors = 0
        for dock in docks:
            try:
                self.db.upsert_dock_status(dock)
                written += 1
            except Exception as exc:
                logger.error("Failed to persist dock %s: %s", dock.dock_id, exc)
                errors += 1
        self.db.log_sync("dock_status", written, errors, started)
        logger.info("Dock sync: %d written, %d errors", written, errors)
        return written

    def sync_events(self) -> int:
        """Poll events since last poll and persist new ones. Returns new events written."""
        started = time.time()
        events = self.client.poll_events(since_timestamp=self._last_poll_ts())
        written = 0
        for event in events:
            is_new = self.db.insert_event(event)
            if is_new:
                written += 1
                if self.event_callback:
                    try:
                        self.event_callback(event)
                    except Exception as exc:
                        logger.error("Event callback error: %s", exc)
        if events:
            self._last_event_poll = max(e.received_at for e in events)
        self.db.log_sync("events", written, 0, started)
        return written

    def _last_poll_ts(self) -> Optional[float]:
        return self._last_event_poll if self._last_event_poll > 0 else None

    def push_trailer_position(self, trailer_id: str, zone: str,
                              dock_id: Optional[str] = None) -> bool:
        pos = TrailerPosition(
            trailer_id=trailer_id,
            dock_id=dock_id,
            zone=zone,
            timestamp=time.time(),
        )
        return self.client.update_trailer_position(pos)

    def _sync_loop(self) -> None:
        logger.info("YMS sync loop started (interval=%ss)", self.poll_interval_s)
        while self._running:
            try:
                self.sync_dock_statuses()
                self.sync_events()
            except Exception as exc:
                logger.error("Sync loop error: %s", exc)
                self._sync_errors += 1
            time.sleep(self.poll_interval_s)

    def start(self) -> None:
        self._running = True
        self.client.connect()
        self._thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._thread.start()
        logger.info("YMS integration service started.")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("YMS integration service stopped.")

    def status_report(self) -> Dict:
        return {
            "running": self._running,
            "sync_errors": self._sync_errors,
            "events_stored": self.db.get_event_count(),
            "last_event_poll": datetime.utcfromtimestamp(self._last_event_poll).isoformat()
                               if self._last_event_poll else "never",
        }


if __name__ == "__main__":
    config = YMSConfig(yard_id="YARD_DEMO")
    client = YMSClient(config)
    db = LocalDB(db_path=":memory:")

    def event_printer(event: YMSEvent):
        print(f"  [EVENT] {event.event_type}: {event.payload}")

    service = YMSIntegrationService(
        yms_client=client,
        local_db=db,
        poll_interval_s=5.0,
        event_callback=event_printer,
    )

    client.connect()
    print("Running one-shot sync...")
    docks_written = service.sync_dock_statuses()
    events_written = service.sync_events()
    print(f"Docks synced: {docks_written}")
    print(f"Events synced: {events_written}")
    print("Status:", service.status_report())
    recent = db.get_recent_dock_statuses(limit=3)
    print(f"\nRecent dock records: {len(recent)}")
    for r in recent:
        print(f"  {r['dock_id']}: {r['status']} trailer={r['trailer_id']}")
