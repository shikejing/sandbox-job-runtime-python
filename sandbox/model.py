"""Domain model: job states, failure reasons, resource limits and the Job entity."""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import asdict, dataclass, field


class JobState(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED})

# The single source of truth for the monotonic state machine.
# queued   -> running | succeeded? (no) | failed | cancelled
# running  -> succeeded | failed | cancelled
_LEGAL_TRANSITIONS: dict[JobState, frozenset[JobState]] = {
    JobState.QUEUED: frozenset({JobState.RUNNING, JobState.FAILED, JobState.CANCELLED}),
    JobState.RUNNING: frozenset(
        {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED}
    ),
    JobState.SUCCEEDED: frozenset(),
    JobState.FAILED: frozenset(),
    JobState.CANCELLED: frozenset(),
}


class FailureReason(str, enum.Enum):
    WALL_TIMEOUT = "wall_timeout"
    CPU_TIME_EXCEEDED = "cpu_time_exceeded"
    MEMORY_EXCEEDED = "memory_exceeded"
    OUTPUT_EXCEEDED = "output_exceeded"
    NONZERO_EXIT = "nonzero_exit"
    SIGNAL_TERMINATED = "signal_terminated"
    CANCELLED = "cancelled"
    NETWORK_POLICY_DENIED = "network_policy_denied"
    INTERNAL_ERROR = "internal_error"
    INTERRUPTED_BY_RESTART = "interrupted_by_restart"


@dataclass(frozen=True)
class ResourceLimits:
    wall_time_s: int
    cpu_time_s: int
    memory_mb: int
    output_bytes: int

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "ResourceLimits":
        return ResourceLimits(**data)


@dataclass
class JobResult:
    exit_code: int | None = None
    signal: int | None = None
    failure_reason: str | None = None
    stdout: str = ""
    stderr: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    timed_out: bool = False
    started_at: float | None = None
    finished_at: float | None = None
    # Resource usage observed while the job ran.
    peak_rss_mb: float = 0.0
    cpu_time_s: float = 0.0
    wall_time_s: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "JobResult":
        known = {f for f in JobResult.__dataclass_fields__}
        return JobResult(**{k: v for k, v in data.items() if k in known})


@dataclass
class IsolationInfo:
    pid_namespace: bool = False
    net_namespace: bool = False
    network_isolated: bool = False
    privilege_dropped: bool = False
    cgroup_memory: bool = False
    degraded: bool = False
    degradation_reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "IsolationInfo":
        known = {f for f in IsolationInfo.__dataclass_fields__}
        return IsolationInfo(**{k: v for k, v in data.items() if k in known})


@dataclass
class Job:
    job_id: str
    submitter: str
    argv: list[str]
    state: JobState = JobState.QUEUED
    request_network: bool = False
    limits: ResourceLimits | None = None
    created_at: float = 0.0
    queued_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None
    result: JobResult | None = None
    isolation: IsolationInfo = field(default_factory=IsolationInfo)
    workdir: str = ""
    env: dict[str, str] = field(default_factory=dict)
    cancel_requested: bool = False
    state_version: int = 0

    @staticmethod
    def new(
        submitter: str,
        argv: list[str],
        limits: ResourceLimits,
        request_network: bool = False,
        env: dict[str, str] | None = None,
    ) -> "Job":
        now = time.time()
        return Job(
            job_id=uuid.uuid4().hex,
            submitter=submitter,
            argv=list(argv),
            state=JobState.QUEUED,
            request_network=request_network,
            limits=limits,
            created_at=now,
            queued_at=now,
            env=dict(env or {}),
        )

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "submitter": self.submitter,
            "argv": list(self.argv),
            "state": self.state.value,
            "request_network": self.request_network,
            "limits": self.limits.to_dict() if self.limits else None,
            "created_at": self.created_at,
            "queued_at": self.queued_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result.to_dict() if self.result else None,
            "isolation": self.isolation.to_dict(),
            "workdir": self.workdir,
            "env": dict(self.env),
            "cancel_requested": self.cancel_requested,
            "state_version": self.state_version,
        }

    @staticmethod
    def from_dict(data: dict) -> "Job":
        return Job(
            job_id=data["job_id"],
            submitter=data["submitter"],
            argv=list(data.get("argv", [])),
            state=JobState(data["state"]),
            request_network=bool(data.get("request_network", False)),
            limits=(
                ResourceLimits.from_dict(data["limits"])
                if data.get("limits") is not None
                else None
            ),
            created_at=float(data.get("created_at", 0.0)),
            queued_at=float(data.get("queued_at", 0.0)),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            result=(
                JobResult.from_dict(data["result"])
                if data.get("result") is not None
                else None
            ),
            isolation=IsolationInfo.from_dict(data.get("isolation", {}) or {}),
            workdir=data.get("workdir", ""),
            env=dict(data.get("env", {})),
            cancel_requested=bool(data.get("cancel_requested", False)),
            state_version=int(data.get("state_version", 0)),
        )

    def public_view(self) -> dict:
        """Representation returned by the API (includes captured output)."""
        view = self.to_dict()
        return view


def check_transition(old: JobState, new: JobState) -> None:
    """Raise ValueError if *old -> new* is not a legal monotonic transition."""
    if old == new:
        raise ValueError(f"no-op transition {old.value}")
    if new not in _LEGAL_TRANSITIONS.get(old, frozenset()):
        raise ValueError(f"illegal state transition {old.value} -> {new.value}")
