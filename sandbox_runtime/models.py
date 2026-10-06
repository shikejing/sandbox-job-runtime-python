"""Core data models: job states, specs, results."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class JobStatus(str, enum.Enum):
    """Lifecycle states. Transitions are monotonic along QUEUED -> RUNNING ->
    one of the terminal states."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in _TERMINAL


_TERMINAL = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED})


class FailureKind(str, enum.Enum):
    """Classified, distinguishable failure reasons."""

    NON_ZERO_EXIT = "non_zero_exit"
    SIGNALLED = "signalled"
    WALL_TIMEOUT = "wall_timeout"
    CPU_LIMIT = "cpu_limit"
    MEMORY_LIMIT = "memory_limit"
    OUTPUT_LIMIT = "output_limit"
    SPAWN_ERROR = "spawn_error"
    INTERNAL_ERROR = "internal_error"


class IsolationMode(str, enum.Enum):
    """How strongly the worker process is isolated."""

    FULL = "full"  # drop privileges + restricted env + dedicated workdir
    DEGRADED = "degraded"  # privilege drop unavailable; explicitly observable


@dataclass(frozen=True)
class ResourceLimits:
    wall_timeout_s: float = 30.0
    cpu_time_s: float = 20.0
    memory_bytes: int = 256 * 1024 * 1024
    max_output_bytes: int = 1024 * 1024


@dataclass(frozen=True)
class JobSpec:
    submitter: str
    command: list[str]
    env: dict[str, str] = field(default_factory=dict)
    limits: ResourceLimits = field(default_factory=ResourceLimits)
    allow_network: bool = False


@dataclass
class JobResult:
    job_id: str
    status: JobStatus
    exit_code: int | None = None
    signal: int | None = None
    failure_kind: FailureKind | None = None
    stdout: str = ""
    stderr: str = ""
    isolation: IsolationMode | None = None
    network_granted: bool = False
    error: str | None = None
    notes: list[str] = field(default_factory=list)
