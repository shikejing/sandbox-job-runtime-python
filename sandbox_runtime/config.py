"""Service configuration with local-single-machine defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    data_dir: str = field(
        default_factory=lambda: os.environ.get("SANDBOX_DATA_DIR", "./sandbox_data")
    )
    global_concurrency: int = int(os.environ.get("SANDBOX_GLOBAL_CONCURRENCY", "4"))
    per_submitter_concurrency: int = int(
        os.environ.get("SANDBOX_PER_SUBMITTER_CONCURRENCY", "2")
    )
    # Service policy: network is denied unless explicitly allowed here AND
    # requested by the submitter.
    network_policy_allow: bool = os.environ.get(
        "SANDBOX_NETWORK_POLICY_ALLOW", "0"
    ) == "1"
    # UID to drop to when running as root; None disables privilege drop.
    drop_uid: int | None = None
    drop_gid: int | None = None
    max_wall_timeout_s: float = 300.0
    max_cpu_time_s: float = 300.0
    max_memory_bytes: int = 2 * 1024 * 1024 * 1024
    max_output_bytes: int = 16 * 1024 * 1024
