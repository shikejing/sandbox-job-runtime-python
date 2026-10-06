"""Durable job store (SQLite) for restart recovery.

Guarantees:
- Every state change is a row update guarded by an explicit allowed-transition
  table, so status only moves forward (QUEUED -> RUNNING -> terminal).
- Terminal rows are never rewritten, so a query always observes one complete,
  consistent state of a submitted job.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import asdict

from .models import (
    FailureKind,
    IsolationMode,
    JobResult,
    JobSpec,
    JobStatus,
    ResourceLimits,
)

_ALLOWED: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
    ),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id     TEXT PRIMARY KEY,
    seq        INTEGER NOT NULL,
    submitter  TEXT NOT NULL,
    spec_json  TEXT NOT NULL,
    status     TEXT NOT NULL,
    result_json TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


def _spec_to_json(spec: JobSpec) -> str:
    return json.dumps(asdict(spec))


def _spec_from_json(raw: str) -> JobSpec:
    d = json.loads(raw)
    d["limits"] = ResourceLimits(**d["limits"])
    return JobSpec(**d)


def _result_to_json(result: JobResult) -> str:
    d = asdict(result)
    d["status"] = result.status.value
    d["failure_kind"] = result.failure_kind.value if result.failure_kind else None
    d["isolation"] = result.isolation.value if result.isolation else None
    return json.dumps(d)


def _result_from_json(job_id: str, status: str, raw: str | None) -> JobResult:
    if raw is None:
        return JobResult(job_id=job_id, status=JobStatus(status))
    d = json.loads(raw)
    d["status"] = JobStatus(d["status"])
    if d.get("failure_kind"):
        d["failure_kind"] = FailureKind(d["failure_kind"])
    if d.get("isolation"):
        d["isolation"] = IsolationMode(d["isolation"])
    return JobResult(**d)


class JobStore:
    """Thread-safe SQLite store. States only move forward."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    def open(self) -> None:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.executescript(_SCHEMA)
        with self._lock:
            self._conn = conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _require_conn(self) -> sqlite3.Connection:
        assert self._conn is not None, "store not opened"
        return self._conn

    def insert_queued(self, job_id: str, spec: JobSpec, seq: int) -> None:
        now = time.time()
        with self._lock:
            self._require_conn().execute(
                "INSERT INTO jobs (job_id, seq, submitter, spec_json, status,"
                " result_json, created_at, updated_at)"
                " VALUES (?,?,?,?,?,NULL,?,?)",
                (job_id, seq, spec.submitter, _spec_to_json(spec),
                 JobStatus.QUEUED.value, now, now),
            )
            self._require_conn().commit()

    def transition(
        self, job_id: str, to: JobStatus, result: JobResult | None = None
    ) -> bool:
        """Monotonic transition; False if illegal or already terminal."""
        allowed_from = [s.value for s, targets in _ALLOWED.items() if to in targets]
        if not allowed_from:
            return False
        placeholders = ",".join("?" for _ in allowed_from)
        result_json = _result_to_json(result) if result is not None else None
        with self._lock:
            cur = self._require_conn().execute(
                f"UPDATE jobs SET status=?, result_json=COALESCE(?, result_json),"
                f" updated_at=? WHERE job_id=? AND status IN ({placeholders})",
                (to.value, result_json, time.time(), job_id, *allowed_from),
            )
            self._require_conn().commit()
            return cur.rowcount == 1

    def get(self, job_id: str) -> JobResult | None:
        with self._lock:
            row = self._require_conn().execute(
                "SELECT status, result_json FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
        if row is None:
            return None
        return _result_from_json(job_id, row[0], row[1])

    def get_spec(self, job_id: str) -> JobSpec | None:
        with self._lock:
            row = self._require_conn().execute(
                "SELECT spec_json FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
        return _spec_from_json(row[0]) if row else None

    def unfinished(self) -> list[str]:
        """Job ids left queued/running at load time (crash recovery)."""
        with self._lock:
            rows = self._require_conn().execute(
                "SELECT job_id FROM jobs WHERE status IN (?,?) ORDER BY seq",
                (JobStatus.QUEUED.value, JobStatus.RUNNING.value),
            ).fetchall()
        return [r[0] for r in rows]

    def requeue(self, job_id: str) -> bool:
        """Crash recovery only: move a stale RUNNING row back to QUEUED."""
        with self._lock:
            cur = self._require_conn().execute(
                "UPDATE jobs SET status=?, updated_at=? WHERE job_id=? AND status=?",
                (JobStatus.QUEUED.value, time.time(), job_id,
                 JobStatus.RUNNING.value),
            )
            self._require_conn().commit()
            return cur.rowcount == 1

    def next_seq(self) -> int:
        """Monotonically increasing submission sequence (survives restart)."""
        with self._lock:
            conn = self._require_conn()
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('seq', 0)"
                " ON CONFLICT(key) DO NOTHING"
            )
            conn.execute("UPDATE meta SET value = value + 1 WHERE key='seq'")
            conn.commit()
            row = conn.execute(
                "SELECT value FROM meta WHERE key='seq'"
            ).fetchone()
            return int(row[0])
