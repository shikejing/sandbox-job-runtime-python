"""Submit/query/list, monotonic states, validation and capabilities."""

from __future__ import annotations

import time

from fastapi.testclient import TestClient

from .conftest import wait_state


def test_health_and_capabilities(client: TestClient):
    h = client.get("/health").json()
    assert h["status"] == "ok"
    caps = client.get("/capabilities").json()
    # The default path must document the network posture honestly.
    assert caps["network_default"] == "closed"
    assert caps["allow_network"] is False
    # Every field the UI/docs rely on is present.
    for key in (
        "pid_namespace",
        "net_namespace",
        "setrlimit",
        "privilege_drop",
        "degradation",
    ):
        assert key in caps


def test_submit_success_returns_full_state(client: TestClient):
    r = client.post(
        "/jobs",
        json={"submitter": "alice", "argv": ["python3", "-c", "print('ok')"]},
    )
    assert r.status_code == 201
    job = r.json()
    assert job["state"] == "queued"
    assert job["submitter"] == "alice"
    assert job["state_version"] == 0
    final = wait_state(client, job["job_id"])
    assert final["state"] == "succeeded"
    assert final["result"]["exit_code"] == 0
    assert final["result"]["stdout"].strip() == "ok"
    assert final["state_version"] >= 2  # queued -> running -> succeeded


def test_get_unknown_job_404(client: TestClient):
    assert client.get("/jobs/does-not-exist").status_code == 404


def test_list_filters_by_submitter(client: TestClient):
    for who in ("a", "a", "b"):
        client.post("/jobs", json={"submitter": who, "argv": ["true"]})
    time.sleep(0.3)
    a = client.get("/jobs", params={"submitter": "a"}).json()["jobs"]
    b = client.get("/jobs", params={"submitter": "b"}).json()["jobs"]
    assert len(a) == 2 and len(b) == 1
    assert all(j["submitter"] == "a" for j in a)


def test_invalid_requests(client: TestClient):
    assert client.post("/jobs", json={"submitter": "x", "argv": []}).status_code == 422
    assert (
        client.post(
            "/jobs", json={"submitter": "", "argv": ["true"]}
        ).status_code
        == 422
    )
    # Limits cannot be negative/zero.
    assert (
        client.post(
            "/jobs",
            json={
                "submitter": "x",
                "argv": ["true"],
                "limits": {"wall_time_s": 0},
            },
        ).status_code
        == 422
    )


def test_state_monotonic_observed(client: TestClient):
    """Polling must only ever observe complete states, in legal order."""
    r = client.post(
        "/jobs",
        json={
            "submitter": "alice",
            "argv": ["python3", "-c", "import time;time.sleep(0.4)"],
        },
    )
    jid = r.json()["job_id"]
    order = []
    deadline = time.time() + 10
    while time.time() < deadline:
        state = client.get(f"/jobs/{jid}").json()["state"]
        if not order or order[-1] != state:
            order.append(state)
        if state in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(0.01)
    rank = {"queued": 0, "running": 1, "succeeded": 2, "failed": 2, "cancelled": 2}
    assert order[0] == "queued"
    assert order[-1] == "succeeded"
    assert all(rank[b] >= rank[a] for a, b in zip(order, order[1:]))


def test_requested_limits_are_clamped_to_ceiling(client: TestClient):
    r = client.post(
        "/jobs",
        json={
            "submitter": "a",
            "argv": ["true"],
            "limits": {
                "wall_time_s": 100000,
                "cpu_time_s": 100000,
                "memory_mb": 100000,
                "output_bytes": 10 ** 9,
            },
        },
    )
    assert r.status_code == 201
    lim = r.json()["limits"]
    assert lim["wall_time_s"] == client._config.max_wall_time_limit_s
    assert lim["cpu_time_s"] == client._config.max_cpu_time_limit_s
    assert lim["memory_mb"] == client._config.max_memory_limit_mb
    assert lim["output_bytes"] == client._config.max_output_limit_bytes
