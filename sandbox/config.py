"""Service configuration loaded from environment variables.

Every knob has a safe default so the default path works on a single local
machine without any external service.  See ``docs/configuration.md`` for the
human-readable description and security notes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_STATE_DIR = "/tmp/sandbox-jobs"


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    return raw if raw not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    """Static configuration for the sandbox service."""

    state_dir: str
    max_global_concurrency: int
    max_per_submitter_concurrency: int
    # Default limits applied when a submitter omits a dimension.
    default_cpu_time_limit_s: int
    default_wall_time_limit_s: int
    default_memory_limit_mb: int
    default_output_limit_bytes: int
    # Hard ceilings a submitter cannot exceed.
    max_cpu_time_limit_s: int
    max_wall_time_limit_s: int
    max_memory_limit_mb: int
    max_output_limit_bytes: int
    cleanup_workdirs: bool
    scheduler_tick_ms: int
    poll_interval_s: float
    # Master switch for the "submitter requests network" feature.  Default
    # deny: even when a network namespace can be provided, jobs run without
    # network unless the operator explicitly opts in.
    allow_network: bool
    # When the platform cannot provide network isolation, requested network
    # access is refused regardless; this knob can additionally permit it
    # (still never silent -- the job is flagged degraded).
    allow_network_on_degraded_isolation: bool
    # Preferred unprivileged account name; None == auto-detect / auto-create.
    runner_user: str | None

    @staticmethod
    def from_env() -> "Config":
        return Config(
            state_dir=_env_str("SANDBOX_STATE_DIR", DEFAULT_STATE_DIR),
            max_global_concurrency=_env_int("SANDBOX_MAX_GLOBAL_CONCURRENCY", 4),
            max_per_submitter_concurrency=_env_int(
                "SANDBOX_MAX_PER_SUBMITTER_CONCURRENCY", 2
            ),
            default_cpu_time_limit_s=_env_int("SANDBOX_DEFAULT_CPU_TIME_S", 10),
            default_wall_time_limit_s=_env_int("SANDBOX_DEFAULT_WALL_TIME_S", 30),
            default_memory_limit_mb=_env_int("SANDBOX_DEFAULT_MEMORY_MB", 256),
            default_output_limit_bytes=_env_int(
                "SANDBOX_DEFAULT_OUTPUT_BYTES", 1 * 1024 * 1024
            ),
            max_cpu_time_limit_s=_env_int("SANDBOX_MAX_CPU_TIME_S", 300),
            max_wall_time_limit_s=_env_int("SANDBOX_MAX_WALL_TIME_S", 600),
            max_memory_limit_mb=_env_int("SANDBOX_MAX_MEMORY_MB", 2048),
            max_output_limit_bytes=_env_int(
                "SANDBOX_MAX_OUTPUT_BYTES", 16 * 1024 * 1024
            ),
            cleanup_workdirs=_env_bool("SANDBOX_CLEANUP_WORKDIRS", True),
            scheduler_tick_ms=_env_int("SANDBOX_SCHEDULER_TICK_MS", 50),
            poll_interval_s=float(_env_int("SANDBOX_POLL_INTERVAL_MS", 50)) / 1000.0,
            allow_network=_env_bool("SANDBOX_ALLOW_NETWORK", False),
            allow_network_on_degraded_isolation=_env_bool(
                "SANDBOX_ALLOW_NETWORK_ON_DEGRADED_ISOLATION", False
            ),
            runner_user=(
                os.environ.get("SANDBOX_RUNNER_USER")
                if os.environ.get("SANDBOX_RUNNER_USER") not in (None, "")
                else None
            ),
        )


def load_config() -> Config:
    return Config.from_env()
