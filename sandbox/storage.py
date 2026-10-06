"""Durable job store: atomic per-job state files with monotonic versioning."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

from .model import (
    TERMINAL_STATES,
    Job,
    JobResult,
    JobState,
    check_transition,
)


class JobExistsError(Exception):
    """Raised when a job id is inserted twice."""


class InvalidTransitionError(Exception):
    """Raised on a non-monotonic state transition."""


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class JobStore:
    """In-memory index backed by JSON files written atomically.

    Each job lives in ``<state_dir>/jobs/<job_id>/state.json``.  Writes use
    temp-file + fsync + rename so a crash leaves either the previous complete
    state or the next complete state, never a torn document.  A monotonic
    ``state_version`` is persisted alongside the state.
    """

    def __init__(self, state_dir: str | Path):
        self.state_dir = Path(state_dir)
        self.jobs_dir = self.state_dir / "jobs"
        self._jobs: dict[str, Job] = {}
        self._lock = threading.RLock()

    # ---- lifecycle -------------------------------------------------------
    def init_dirs(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

    def job_dir(self, job_id: str) -> Path:
        return self.jobs_dir / job_id

    def workdir(self, job_id: str) -> Path:
        d = self.job_dir(job_id) / "work"
        return d

    def recover(self) -> tuple[list[Job], list[Job]]:
        """Load every persisted job.

        Returns ``(finished, interrupted)`` where *interrupted* lists jobs
        that were queued or running when the previous process stopped.  The
        caller decides how to reconcile them; this layer only loads facts.
        """
        finished: list[Job] = []
        interrupted: list[Job] = []
        with self._lock:
            self._jobs.clear()
            if not self.jobs_dir.exists():
                return finished, interrupted
            for child in sorted(self.jobs_dir.iterdir()):
                state_file = child / "state.json"
                if not state_file.is_file():
                    continue
                try:
                    with state_file.open("r", encoding="utf-8") as fh:
                        data = json.load(fh)
                    job = Job.from_dict(data)
                except (OSError, ValueError, KeyError):
                    # A torn/unknown file is ignored rather than crashing boot.
                    continue
                self._jobs[job.job_id] = job
                if job.state in TERMINAL_STATES:
                    finished.append(job)
                else:
                    interrupted.append(job)
            return finished, interrupted

    # ---- mutations -------------------------------------------------------
    def create(self, job: Job) -> None:
        with self._lock:
            if job.job_id in self._jobs:
                raise JobExistsError(job.job_id)
            self._jobs[job.job_id] = job
            self._persist(job)

    def request_cancel(self, job_id: str) -> bool:
        """Mark cancel_requested. Returns False for unknown/terminal jobs."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.state in TERMINAL_STATES:
                return False
            if not job.cancel_requested:
                job.cancel_requested = True
                self._persist(job)
            return True

    def transition(
        self,
        job_id: str,
        new_state: JobState,
        result: JobResult | None = None,
        isolation: Any = None,
        started: bool = False,
        finished: bool = False,
    ) -> Job:
        """Apply a monotonic transition, bump the version and persist."""
        import time

        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            check_transition(job.state, new_state)  # raises InvalidTransition-ish
            job.state = new_state
            job.state_version += 1
            now = time.time()
            if started:
                job.started_at = now
            if finished:
                job.finished_at = now
            if result is not None:
                job.result = result
            if isolation is not None:
                job.isolation = isolation
            self._persist(job)
            return job

    def force_transition(
        self, job_id: str, new_state: JobState, result: JobResult | None = None
    ) -> Job:
        """Crash-recovery-only transition that bypasses the monotonic machine.

        Used exclusively when reconciling a job that was RUNNING in a previous
        (dead) process: that execution can never resume or be observed, so its
        stale ``running`` fact must be reconciled to a terminal state.
        """
        import time

        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            job.state = new_state
            job.state_version += 1
            job.finished_at = time.time()
            if result is not None:
                job.result = result
            self._persist(job)
            return job

    # ---- reads -----------------------------------------------------------
    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self, submitter: str | None = None) -> list[Job]:
        with self._lock:
            jobs = list(self._jobs.values())
        if submitter is not None:
            jobs = [j for j in jobs if j.submitter == submitter]
        jobs.sort(key=lambda j: (j.queued_at, j.job_id))
        return jobs

    # ---- persistence -----------------------------------------------------
    def _state_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "state.json"

    def _persist(self, job: Job) -> None:
        job_dir = self.job_dir(job.job_id)
        job_dir.mkdir(parents=True, exist_ok=True)
        target = job_dir / "state.json"
        data = json.dumps(job.to_dict(), ensure_ascii=False, indent=2)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".state.", suffix=".tmp", dir=str(job_dir)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, target)
            _fsync_dir(job_dir)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
