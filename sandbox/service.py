"""Service lifecycle: wires config, store, capability probe and scheduler."""

from __future__ import annotations

import logging
import time

from .config import Config, load_config
from .isolation import IsolationCapabilities, probe_capabilities
from .model import FailureReason, JobResult, JobState
from .scheduler import Scheduler
from .storage import JobStore

LOG = logging.getLogger("sandbox.service")


class SandboxService:
    """Owns the long-lived components and their startup/shutdown order."""

    def __init__(self, config: Config | None = None):
        self.config: Config = config if config is not None else load_config()
        self.caps: IsolationCapabilities | None = None
        self.store: JobStore | None = None
        self.scheduler: Scheduler | None = None
        self.started_at: float = 0.0
        self.recovered_finished: int = 0
        self.recovered_interrupted: int = 0

    def start(self) -> "SandboxService":
        self.store = JobStore(self.config.state_dir)
        self.store.init_dirs()

        # Probe isolation capabilities honestly, before accepting work.
        self.caps = probe_capabilities(self.config.runner_user)
        for line in self.caps.describe():
            LOG.info("isolation: %s", line)

        self._recover()

        self.scheduler = Scheduler(self.store, self.config, self.caps)
        self.scheduler.start()
        self.started_at = time.time()
        return self

    def _recover(self) -> None:
        """Align state across restart.

        Already-terminal jobs are loaded as facts and their stored results are
        returned verbatim -- they are never re-executed.  Jobs that were
        QUEUED or RUNNING when the previous process stopped are moved to a
        terminal FAILED state with the distinct reason
        ``interrupted_by_restart``: they are neither silently re-run nor ever
        reported as successful.  Submitters may resubmit them.
        """
        assert self.store is not None
        finished, interrupted = self.store.recover()
        self.recovered_finished = len(finished)
        self.recovered_interrupted = len(interrupted)
        now = time.time()
        for job in interrupted:
            result = JobResult(
                failure_reason=FailureReason.INTERRUPTED_BY_RESTART.value,
                stderr=(
                    "job was %s when the service restarted; it was not "
                    "re-executed and is not reported as successful. Resubmit "
                    "to run it again." % job.state.value
                ),
                finished_at=now,
            )
            # Every unfinished job (queued or running) becomes a terminal
            # FAILED with the distinct interrupted_by_restart reason.  QUEUED
            # jobs use the normal monotonic transition; RUNNING jobs need the
            # recovery-only force path.
            if job.state is JobState.QUEUED:
                self.store.transition(
                    job.job_id, JobState.FAILED, result=result, finished=True
                )
            else:
                self.store.force_transition(
                    job.job_id, JobState.FAILED, result=result
                )
        if interrupted:
            LOG.warning(
                "recovery: %d finished, %d interrupted reconciled",
                len(finished),
                len(interrupted),
            )

    def abort(self) -> None:
        """Simulate an abrupt process death (tests / SIGKILL semantics).

        Running process trees are hard-killed and worker threads abandoned,
        while persisted job states are deliberately left QUEUED/RUNNING so the
        following start's recovery path has real interrupted work to reconcile.
        """
        if self.scheduler is None or self.store is None:
            return
        sched = self.scheduler
        sched.abandon()
        self.scheduler = None

    def shutdown(self) -> None:
        if self.scheduler is not None:
            self.scheduler.stop(timeout=5.0)
            self.scheduler = None
