"""Shared fixtures: an isolated service instance with a temp state dir."""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from sandbox.config import Config
from sandbox.isolation import probe_capabilities
from sandbox.service import SandboxService


def _make_config(tmp_path: str, **overrides) -> Config:
    base = Config(
        state_dir=tmp_path,
        max_global_concurrency=overrides.get("max_global_concurrency", 4),
        max_per_submitter_concurrency=overrides.get(
            "max_per_submitter_concurrency", 2
        ),
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
    return base


@pytest.fixture()
def client(tmp_path):
    cfg = _make_config(str(tmp_path))
    service = SandboxService(cfg).start()
    app = __import__("sandbox.api", fromlist=["create_app"]).create_app(service)
    with TestClient(app) as c:
        c._service = service  # type: ignore[attr-defined]
        c._config = cfg  # type: ignore[attr-defined]
        yield c
    service.shutdown()


@pytest.fixture()
def client_factory(tmp_path):
    """Build fresh services sharing one state dir (restart simulation)."""
    created: list[SandboxService] = []

    def _factory(**overrides):
        cfg = _make_config(str(tmp_path), **overrides)
        service = SandboxService(cfg).start()
        app = __import__("sandbox.api", fromlist=["create_app"]).create_app(service)
        created.append(service)
        return TestClient(app), service, cfg

    yield _factory
    for service in created:
        service.shutdown()


def wait_state(client, job_id: str, terminal=True, timeout: float = 20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = client.get(f"/jobs/{job_id}").json()
        if not terminal or data["state"] in (
            "succeeded",
            "failed",
            "cancelled",
        ):
            return data
        time.sleep(0.03)
    raise AssertionError(f"job {job_id} did not finish within {timeout}s")


@pytest.fixture()
def caps():
    return probe_capabilities()
