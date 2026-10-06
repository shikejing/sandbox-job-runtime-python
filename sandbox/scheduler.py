"""FCFS scheduler with global and per-submitter concurrency caps."""

from __future__ import annotations

import logging
import threading

from .config import Config
from .isolation import IsolationCapabilities
from .model import FailureReason, JobResult, JobState
from .runner import ProcessRunner
from .storage import JobStore

LOG = logging.getLogger("sandbox.scheduler")


class Scheduler:
    """Picks queued jobs first-come-first-served and runs them in threads.

    One dispatch thread owns ordering.  Admission is gated by two counters:
    global in-flight and per-submitter in-flight.  Every tick the queue is
    scanned in submission order and every *admissible* job is launched, so a
    submitter with a long-running job only blocks itself -- later submitters
    and even that same submitter's *other* admissible jobs keep moving up to
    their quota, i.e. no head-of-line starvation beyond declared quotas.
    """

    def __init__(self, store: JobStore, config: Config, caps: IsolationCapabilities):
        self.store = store
        self.config = config
        self.caps = caps
        self.runner = ProcessRunner(config, caps)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._global_inflight = 0
        self._per_submitter: dict[str, int] = {}
        self._counter_lock = threading.Lock()
        self._active: dict[str, threading.Thread] = {}
        self._active_lock = threading.Lock()
        self._tick = max(0.01, config.scheduler_tick_ms / 1000.0)
        # When set, workers skip terminal state writes (crash simulation).
        self._abandoned = threading.Event()

    # ---- lifecycle --------------------------------------------------------
    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="sandbox-scheduler", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        """Signal stop and wait for in-flight jobs; they are killed at teardown."""
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5.0)
        with self._active_lock:
            workers = list(self._active.values())
        # Mark cancel on every running job so their supervisors terminate now.
        for job in self.store.list():
            if job.state is JobState.RUNNING:
                self.store.request_cancel(job.job_id)
        for worker in workers:
            worker.join(timeout=timeout)
        self._thread = None

    def abandon(self) -> None:
        """Crash simulation: stop dispatching, kill running trees, skip writes."""
        self._abandoned.set()
        self._stop.set()
        # Cancel-and-kill every running process so no test process leaks.
        if self.store is not None:
            for job in self.store.list():
                if job.state is JobState.RUNNING:
                    job.cancel_requested = True
        if self._thread is not None:
            self._thread.join(timeout=3.0)
        for worker in list(self._active.values()):
            worker.join(timeout=5.0)

    # ---- dispatch loop ----------------------------------------------------
    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._dispatch()
            except Exception:  # pragma: no cover - defensive
                LOG.exception("scheduler dispatch iteration failed")
            self._stop.wait(self._tick)

    def _dispatch(self) -> None:
        queued = [
            j
            for j in self.store.list()
            if j.state is JobState.QUEUED
        ]
        for job in queued:
            with self._counter_lock:
                # Cancel-while-queued is finalized here, atomically with
                # admission, so the job can never both cancel and start.
                if job.cancel_requested:
                    try:
                        self.store.transition(
                            job.job_id,
                            JobState.CANCELLED,
                            result=JobResult(
                                failure_reason=FailureReason.CANCELLED.value,
                                stderr="cancelled while queued; never executed",
                            ),
                            finished=True,
                        )
                    except Exception:
                        pass
                    continue
                if self._global_inflight >= self.config.max_global_concurrency:
                    return  # global cap hit; reconsider next tick
                per = self._per_submitter.get(job.submitter, 0)
                if per >= self.config.max_per_submitter_concurrency:
                    continue  # this submitter is saturated; try the next job
                # Admit: transition under the same lock to avoid double admits.
                try:
                    self.store.transition(job.job_id, JobState.RUNNING, started=True)
                except Exception:
                    continue
                self._global_inflight += 1
                self._per_submitter[job.submitter] = per + 1

            worker = threading.Thread(
                target=self._execute,
                args=(job.job_id,),
                name=f"job-{job.job_id[:8]}",
                daemon=True,
            )
            with self._active_lock:
                self._active[job.job_id] = worker
            worker.start()

    # ---- worker -----------------------------------------------------------
    def _execute(self, job_id: str) -> None:
        try:
            self._run_one(job_id)
        except Exception as exc:  # absolute isolation from scheduler state
            LOG.exception("worker for %s crashed", job_id)
            self._terminate_internal_error(job_id, exc)
        finally:
            with self._counter_lock:
                self._global_inflight = max(0, self._global_inflight - 1)
                job = self.store.get(job_id)
                if job is not None:
                    n = self._per_submitter.get(job.submitter, 1) - 1
                    if n <= 0:
                        self._per_submitter.pop(job.submitter, None)
                    else:
                        self._per_submitter[job.submitter] = n
            with self._active_lock:
                self._active.pop(job_id, None)

    def _run_one(self, job_id: str) -> None:
        job = self.store.get(job_id)
        if job is None:
            return
        # At this point the job is RUNNING; cancel-while-queued was finalized
        # atomically at the dispatch point and never reaches a worker.
        result = self.runner.run(job)
        if self.config.cleanup_workdirs:
            self.runner.cleanup_workdir(job)
        # If the service "crashed" while we were running, do not write a
        # terminal state: recovery on next start must see the interruption.
        if self._abandoned.is_set():
            return
        target = self._target_state(job, result)
        try:
            self.store.transition(
                job_id,
                target,
                result=result,
                isolation=job.isolation,
                finished=True,
            )
        except Exception:
            # Already terminal (e.g. raced with recovery) -- nothing to do.
            LOG.debug("terminal transition skipped for %s", job_id)

    def _target_state(self, job, result: JobResult) -> JobState:
        if result.failure_reason == FailureReason.CANCELLED.value or (
            job.cancel_requested and result.signal
        ):
            return JobState.CANCELLED
        if result.exit_code == 0 and result.failure_reason is None:
            return JobState.SUCCEEDED
        return JobState.FAILED

    def _terminate_internal_error(self, job_id: str, exc: Exception) -> None:
        job = self.store.get(job_id)
        if job is None or job.state in (
            JobState.SUCCEEDED,
            JobState.FAILED,
            JobState.CANCELLED,
        ):
            return
        if self._abandoned.is_set():
            return
        result = JobResult(
            failure_reason=FailureReason.INTERNAL_ERROR.value,
            stderr=f"internal scheduler/runner error: {exc!r}",
        )
        try:
            self.store.transition(
                job_id, JobState.FAILED, result=result, finished=True
            )
        except Exception:
            LOG.debug("internal-error transition skipped for %s", job_id)

