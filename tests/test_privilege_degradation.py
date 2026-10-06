"""Degradation must be explicit and observable, never silently permissive.

These tests force caps to report "no privilege drop / no namespace" and check
that jobs still run but carry honest isolation metadata, while the network
path stays closed under the default policy.
"""

from __future__ import annotations

import time

from fastapi.testclient import TestClient

from sandbox.api import create_app
from sandbox.config import Config
from sandbox.isolation import IsolationCapabilities
from sandbox.service import SandboxService

from .conftest import wait_state


def _service_with_caps(tmp_path, **caps_overrides):
    cfg = Config(
        state_dir=str(tmp_path),
        max_global_concurrency=2,
        max_per_submitter_concurrency=2,
        default_cpu_time_limit_s=10,
        default_wall_time_limit_s=30,
        default_memory_limit_mb=256,
        default_output_limit_bytes=1024 * 1024,
        max_cpu_time_limit_s=300,
        max_wall_time_limit_s=600,
        max_memory_limit_mb=2048,
        max_output_limit_bytes=16 * 1024 * 1024,
        cleanup_workdirs=True,
        scheduler_tick_ms=20,
        poll_interval_s=0.05,
        allow_network=False,
        allow_network_on_degraded_isolation=False,
        runner_user=None,
    )
    service = SandboxService(cfg)
    service.caps = IsolationCapabilities(**caps_overrides)
    service.store = __import__(
        "sandbox.storage", fromlist=["JobStore"]
    ).JobStore(cfg.state_dir)
    service.store.init_dirs()
    from sandbox.scheduler import Scheduler

    service.scheduler = Scheduler(service.store, cfg, service.caps)
    service.scheduler.start()
    service.started_at = time.time()
    return service


def test_degraded_privilege_drop_is_flagged(tmp_path):
    # No root, no namespaces, no privilege drop: runner must mark the job
    # degraded and name the missing capabilities.
    service = _service_with_caps(
        tmp_path,
        running_as_root=False,
        can_pid_namespace=False,
        can_net_namespace=False,
        can_setrlimit=True,
        can_drop_privileges=False,
        degradation=[
            "pid_namespace_unavailable",
            "net_namespace_unavailable",
            "privilege_drop_unavailable",
        ],
    )
    app = create_app(service)
    try:
        with TestClient(app) as client:
            # Network still closed by policy even though netns is unavailable.
            denied = client.post(
                "/jobs",
                json={"submitter": "u", "argv": ["true"], "request_network": True},
            )
            assert denied.status_code == 403

            jid = client.post(
                "/jobs", json={"submitter": "u", "argv": ["python3", "-c", "print('ran')"]}
            ).json()["job_id"]
            final = wait_state(client, jid)
            assert final["state"] == "succeeded"
            iso = final["isolation"]
            assert iso["degraded"] is True
            assert "privilege_drop_unavailable" in iso["degradation_reasons"]
            assert iso["privilege_dropped"] is False
            # Network isolation cannot be provided: reported, not faked.
            assert "network_isolation_unavailable" in iso["degradation_reasons"]
            assert iso["network_isolated"] is False

            caps = client.get("/capabilities").json()
            assert caps["privilege_drop"]["available"] is False
            assert caps["degradation"]
    finally:
        service.shutdown()
