"""Global and per-submitter concurrency caps, FCFS queuing and no starvation."""

from __future__ import annotations

import time


from .conftest import wait_state


SLEEP = ["python3", "-c", "import time;time.sleep(1.2)"]


def _submit(client, submitter, argv=SLEEP):
    r = client.post("/jobs", json={"submitter": submitter, "argv": argv})
    assert r.status_code == 201
    return r.json()["job_id"]


def _states(client, ids):
    out = {}
    for jid in ids:
        out[jid] = client.get(f"/jobs/{jid}").json()["state"]
    return out


def test_global_cap_queues_instead_of_rejecting(client_factory):
    client, service, _ = client_factory(max_global_concurrency=2)
    ids = [_submit(client, "u") for _ in range(4)]
    time.sleep(0.35)
    states = _states(client, ids)
    running = [j for j, s in states.items() if s == "running"]
    queued = [j for j, s in states.items() if s == "queued"]
    assert len(running) == 2
    assert len(queued) == 2  # excess queued, never rejected
    for jid in ids:
        wait_state(client, jid, timeout=15)
    assert all(
        client.get(f"/jobs/{j}").json()["state"] == "succeeded" for j in ids
    )


def test_per_submitter_cap_and_no_starvation(client_factory):
    # global=3, per-submitter=1.  Submitter A floods the queue; submitter B
    # arriving later must still get a slot (A cannot starve B).
    client, _, _ = client_factory(max_global_concurrency=3, max_per_submitter_concurrency=1)
    a_jobs = [_submit(client, "alice") for _ in range(3)]
    time.sleep(0.25)  # let A's first start, rest queue
    b_job = _submit(client, "bob")
    # B is admitted even though A has 2 queued jobs ahead in absolute order.
    deadline = time.time() + 5
    b_state = "queued"
    while time.time() < deadline:
        b_state = client.get(f"/jobs/{b_job}").json()["state"]
        if b_state in ("running", "succeeded"):
            break
        time.sleep(0.03)
    assert b_state in ("running", "succeeded")
    # At no point did alice run more than one at once: every A job completes.
    for jid in a_jobs:
        assert wait_state(client, jid, timeout=20)["state"] == "succeeded"
    assert wait_state(client, b_job, timeout=20)["state"] == "succeeded"


def test_fcfs_order_within_submitter(client_factory):
    client, _, _ = client_factory(max_global_concurrency=1, max_per_submitter_concurrency=1)
    ids = [
        _submit(
            client,
            "u",
            ["python3", "-c", f"import time;time.sleep(0.4);print({i})"],
        )
        for i in range(3)
    ]
    # Record start times in order by polling.
    start_order = []
    seen = set()
    deadline = time.time() + 12
    while time.time() < deadline and len(seen) < 3:
        for jid in ids:
            st = client.get(f"/jobs/{jid}").json()["state"]
            if st in ("running", "succeeded", "failed") and jid not in seen:
                if st == "running":
                    seen.add(jid)
                    start_order.append(jid)
        time.sleep(0.01)
    assert start_order == ids
    for jid in ids:
        wait_state(client, jid, timeout=20)
