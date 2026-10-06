"""FIFO scheduler with global and per-submitter concurrency caps.

- Submissions are never rejected for load: they are persisted as QUEUED and
  dispatched in submission order (sequence numbers survive restarts).
- Dispatch picks the oldest queued job whose submitter is below the
  per-submitter cap, so one submitter at its cap cannot starve the others
  (first-come-first-served among eligible jobs).
- Cancellation takes effect for both queued and running jobs without
  touching any other job; a cancelled running job's process tree is killed
  by the executor and its working directory removed.
"""

from __future__ import annotations

import threading
import uuid
from collections import deque
from dataclasses import replace

from .config import Settings
from .executor import Executor
from .models import (
    FailureKind,
    JobResult,
    JobSpec,
    JobStatus,
    ResourceLimits,
)
from .store import JobStore


class Scheduler:
    def __init__(
        self, settings: Settings, store: JobStore, executor: Executor
    ) -> None:
        self._settings = settings
        self._store = store
        self._executor = executor
        self._cond = threading.Condition()
        self._queue: deque[str] = deque()
        self._specs: dict[str, JobSpec] = {}
        self._running: dict[str, threading.Event] = {}
        self._running_submitters: dict[str, int] = {}
        self._stop = False
        self._dispatcher: threading.Thread | None = None
        self._workers: set[threading.Thread] = set()

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        self._dispatcher = threading.Thread(
            target=self._dispatch_loop, name="scheduler-dispatch", daemon=True
        )
        self._dispatcher.start()

    def shutdown(self) -> None:
        with self._cond:
            self._stop = True
            for ev in self._running.values():
                ev.set()  # ask running jobs to stop
            self._cond.notify_all()
        if self._dispatcher is not None:
            self._dispatcher.join(timeout=10)
        for t in list(self._workers):
            t.join(timeout=30)

    # -- submission --------------------------------------------------------

    def _clamp_limits(self, spec: JobSpec) -> JobSpec:
        s = self._settings
        lim = spec.limits
        clamped = ResourceLimits(
            wall_timeout_s=min(lim.wall_timeout_s, s.max_wall_timeout_s),
            cpu_time_s=min(lim.cpu_time_s, s.max_cpu_time_s),
            memory_bytes=min(lim.memory_bytes, s.max_memory_bytes),
            max_output_bytes=min(lim.max_output_bytes, s.max_output_bytes),
        )
        return replace(spec, limits=clamped)

    def submit(self, spec: JobSpec) -> str:
        """Enqueue; never rejects due to load. Returns the job id."""
        spec = self._clamp_limits(spec)
        job_id = uuid.uuid4().hex
        seq = self._store.next_seq()
        self._store.insert_queued(job_id, spec, seq)
        with self._cond:
            self._specs[job_id] = spec
            self._queue.append(job_id)
            self._cond.notify_all()
        return job_id

    # -- cancellation ------------------------------------------------------

    def cancel(self, job_id: str) -> bool:
        with self._cond:
            if job_id in self._queue:
                self._queue.remove(job_id)
                self._specs.pop(job_id, None)
                result = JobResult(job_id=job_id, status=JobStatus.CANCELLED)
                return self._store.transition(
                    job_id, JobStatus.CANCELLED, result
                )
            ev = self._running.get(job_id)
            if ev is not None:
                ev.set()  # worker records the CANCELLED result
                return True
        return False

    # -- queries -----------------------------------------------------------

    def get(self, job_id: str) -> JobResult | None:
        return self._store.get(job_id)

    # -- recovery ----------------------------------------------------------

    def recover(self) -> None:
        """Requeue jobs left unfinished by a previous process."""
        for job_id in self._store.unfinished():
            spec = self._store.get_spec(job_id)
            if spec is None:
                continue
            self._store.requeue(job_id)  # RUNNING -> QUEUED (crash recovery)
            with self._cond:
                self._specs[job_id] = spec
                self._queue.append(job_id)
        with self._cond:
            self._cond.notify_all()

    # -- dispatch ----------------------------------------------------------

    def _dispatch_loop(self) -> None:
        while True:
            with self._cond:
                self._cond.wait(timeout=0.5)
                while not self._stop:
                    picked = self._pick_locked()
                    if picked is None:
                        break
                    job_id, spec = picked
                    ev = threading.Event()
                    self._running[job_id] = ev
                    self._running_submitters[spec.submitter] = (
                        self._running_submitters.get(spec.submitter, 0) + 1
                    )
                    self._store.transition(job_id, JobStatus.RUNNING)
                    worker = threading.Thread(
                        target=self._run_one,
                        args=(job_id, spec, ev),
                        name=f"job-{job_id[:8]}",
                        daemon=True,
                    )
                    self._workers.add(worker)
                    worker.start()
                if self._stop:
                    return

    def _pick_locked(self) -> tuple[str, JobSpec] | None:
        if len(self._running) >= self._settings.global_concurrency:
            return None
        per_cap = self._settings.per_submitter_concurrency
        for job_id in list(self._queue):
            spec = self._specs.get(job_id)
            if spec is None:
                self._queue.remove(job_id)
                continue
            if self._running_submitters.get(spec.submitter, 0) >= per_cap:
                continue  # head-of-line block avoided: look further back
            self._queue.remove(job_id)
            self._specs.pop(job_id, None)
            return job_id, spec
        return None

    def _run_one(
        self, job_id: str, spec: JobSpec, cancel_event: threading.Event
    ) -> None:
        try:
            network_granted = (
                spec.allow_network and self._settings.network_policy_allow
            )
            result = self._executor.run(job_id, spec, network_granted, cancel_event)
        except Exception as exc:  # last line of defence
            result = JobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                failure_kind=FailureKind.INTERNAL_ERROR,
                error=f"scheduler worker error: {exc!r}",
            )
        try:
            self._store.transition(job_id, result.status, result)
        finally:
            with self._cond:
                self._running.pop(job_id, None)
                n = self._running_submitters.get(spec.submitter, 0)
                if n <= 1:
                    self._running_submitters.pop(spec.submitter, None)
                else:
                    self._running_submitters[spec.submitter] = n - 1
                self._workers.discard(threading.current_thread())
                self._cond.notify_all()
