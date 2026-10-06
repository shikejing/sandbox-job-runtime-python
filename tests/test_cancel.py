"""Cancellation: queued, running, isolation between jobs and resource cleanup."""

from __future__ import annotations

import subprocess
import time

from fastapi.testclient import TestClient

from .conftest import wait_state


def _submit(client, submitter="u", argv=None, **limits):
    body = {
        "submitter": submitter,
        "argv": argv or ["python3", "-c", "import time;time.sleep(30)"],
    }
    if limits:
        body["limits"] = limits
    r = client.post("/jobs", json=body)
    assert r.status_code == 201
    return r.json()["job_id"]


def _procs_with_arg(marker: str):
    out = subprocess.run(
        ["ps", "-eo", "args"], capture_output=True, text=True
    ).stdout
    return [l for l in out.splitlines() if marker in l and "ps -eo" not in l]


def test_cancel_queued_job_never_runs(client_factory):
    # global cap 1: blocker occupies the only slot, victim stays queued.
    client, _, _ = client_factory(max_global_concurrency=1, max_per_submitter_concurrency=1)
    blocker = _submit(client)
    time.sleep(0.25)  # blocker running
    jid = _submit(client)  # must be queued behind the blocker
    assert client.get(f"/jobs/{jid}").json()["state"] == "queued"
    r = client.post(f"/jobs/{jid}/cancel")
    assert r.status_code == 202
    final = wait_state(client, jid)
    assert final["state"] == "cancelled"
    assert final["result"]["failure_reason"] == "cancelled"
    assert final["started_at"] is None  # never executed
    client.post(f"/jobs/{blocker}/cancel")
    wait_state(client, blocker)


def test_cancel_running_job_terminates(client: TestClient):
    jid = _submit(client, argv=["python3", "-c", "import time;time.sleep(30)"], wall_time_s=30)
    time.sleep(0.4)
    assert client.get(f"/jobs/{jid}").json()["state"] == "running"
    r = client.post(f"/jobs/{jid}/cancel")
    assert r.status_code == 202
    final = wait_state(client, jid, timeout=10)
    assert final["state"] == "cancelled"
    assert final["result"]["signal"] == 9


def test_cancel_does_not_affect_other_jobs(client: TestClient):
    victim = _submit(client, "alice", ["python3", "-c", "import time;time.sleep(30)"], wall_time_s=30)
    survivor = _submit(
        client,
        "bob",
        ["python3", "-c", "import time;time.sleep(0.8);print('survived')"],
        wall_time_s=10,
    )
    time.sleep(0.4)
    client.post(f"/jobs/{victim}/cancel")
    assert wait_state(client, victim, timeout=10)["state"] == "cancelled"
    s = wait_state(client, survivor, timeout=15)
    assert s["state"] == "succeeded"
    assert "survived" in s["result"]["stdout"]


def test_cancel_reaps_process_tree(client: TestClient):
    marker_sleep = "77801"
    jid = _submit(
        client,
        "u",
        [
            "python3",
            "-c",
            (
                "import subprocess,time\n"
                f"subprocess.Popen(['sleep','{marker_sleep}'])\n"
                "time.sleep(30)"
            ),
        ],
        wall_time_s=30,
    )
    time.sleep(0.6)
    assert _procs_with_arg(f"sleep {marker_sleep}"), "grandchild should exist"
    client.post(f"/jobs/{jid}/cancel")
    wait_state(client, jid, timeout=10)
    time.sleep(0.4)
    assert not _procs_with_arg(f"sleep {marker_sleep}"), (
        "grandchild process survived cancellation"
    )


def test_cancel_terminal_job_is_noop(client: TestClient):
    jid = _submit(client, argv=["true"])
    wait_state(client, jid)
    r = client.post(f"/jobs/{jid}/cancel")
    assert r.status_code == 202
    assert r.json()["cancel_requested"] is False
    assert client.get(f"/jobs/{jid}").json()["state"] == "succeeded"


def test_cancel_unknown_job_404(client: TestClient):
    assert client.post("/jobs/nope/cancel").status_code == 404
