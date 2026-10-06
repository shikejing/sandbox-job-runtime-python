"""Crash isolation for the scheduler and state alignment across restart."""

from __future__ import annotations

import time


from sandbox.model import JobState

from .conftest import wait_state


def test_worker_crash_does_not_take_down_scheduler(client_factory, monkeypatch):
    client, service, cfg = client_factory()

    def boom(self, job):
        raise RuntimeError("simulated runner explosion")

    monkeypatch.setattr("sandbox.scheduler.ProcessRunner.run", boom)
    bad = client.post("/jobs", json={"submitter": "u", "argv": ["true"]}).json()[
        "job_id"
    ]
    time.sleep(0.4)

    monkeypatch.undo()
    good = wait_state(
        client,
        client.post(
            "/jobs",
            json={"submitter": "u", "argv": ["python3", "-c", "print('alive')"]},
        ).json()["job_id"],
    )
    assert good["state"] == "succeeded"
    assert "alive" in good["result"]["stdout"]

    bad_final = client.get(f"/jobs/{bad}").json()
    assert bad_final["state"] == "failed"
    assert bad_final["result"]["failure_reason"] == "internal_error"


def test_succeeded_jobs_not_reexecuted_on_restart(client_factory):
    client1, service1, _ = client_factory()
    jid = client1.post(
        "/jobs",
        json={"submitter": "alice", "argv": ["python3", "-c", "print('once')"]},
    ).json()["job_id"]
    assert wait_state(client1, jid)["state"] == "succeeded"
    service1.shutdown()

    client2, service2, _ = client_factory()
    got = client2.get(f"/jobs/{jid}").json()
    assert got["state"] == "succeeded"
    assert got["result"]["stdout"].strip() == "once"
    assert service2.recovered_finished == 1
    assert service2.recovered_interrupted == 0


def test_running_and_queued_marked_interrupted_on_restart(client_factory):
    client1, service1, _ = client_factory(max_global_concurrency=1)

    running_id = client1.post(
        "/jobs",
        json={
            "submitter": "a",
            "argv": ["python3", "-c", "import time;time.sleep(30)"],
            "limits": {"wall_time_s": 30},
        },
    ).json()["job_id"]
    deadline = time.time() + 5
    while client1.get(f"/jobs/{running_id}").json()["state"] != "running":
        time.sleep(0.03)
        assert time.time() < deadline

    queued_id = client1.post(
        "/jobs", json={"submitter": "b", "argv": ["true"]}
    ).json()["job_id"]
    time.sleep(0.2)

    # Abrupt death: on-disk states must remain running and queued.
    assert service1.store.get(running_id).state is JobState.RUNNING
    assert service1.store.get(queued_id).state is JobState.QUEUED
    service1.abort()

    client2, service2, _ = client_factory()
    assert service2.recovered_interrupted >= 1
    for jid in (running_id, queued_id):
        got = client2.get(f"/jobs/{jid}").json()
        assert got["state"] == "failed", got["state"]
        assert got["result"]["failure_reason"] == "interrupted_by_restart"
        assert got["state"] != "succeeded"


def test_interrupted_job_can_be_resubmitted_and_succeeds(client_factory):
    client1, service1, _ = client_factory()
    jid = client1.post(
        "/jobs",
        json={
            "submitter": "a",
            "argv": ["python3", "-c", "import time;time.sleep(30)"],
            "limits": {"wall_time_s": 30},
        },
    ).json()["job_id"]
    deadline = time.time() + 5
    while client1.get(f"/jobs/{jid}").json()["state"] != "running":
        time.sleep(0.03)
        assert time.time() < deadline
    service1.abort()

    client2, _, _ = client_factory()
    assert client2.get(f"/jobs/{jid}").json()["state"] == "failed"
    again = wait_state(
        client2,
        client2.post(
            "/jobs",
            json={"submitter": "a", "argv": ["python3", "-c", "print('again')"]},
        ).json()["job_id"],
    )
    assert again["state"] == "succeeded"
    assert "again" in again["result"]["stdout"]


def test_restart_repeatedly_keeps_state_consistent(client_factory):
    client1, service1, _ = client_factory()
    done = wait_state(
        client1,
        client1.post(
            "/jobs", json={"submitter": "a", "argv": ["python3", "-c", "print('x')"]}
        ).json()["job_id"],
    )
    assert done["state"] == "succeeded"
    service1.shutdown()

    for _ in range(2):
        c, s, _ = client_factory()
        assert s.recovered_finished == 1
        assert s.recovered_interrupted == 0
        s.shutdown()
