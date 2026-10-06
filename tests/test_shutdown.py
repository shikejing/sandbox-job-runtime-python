"""Graceful shutdown reclaims running work; abrupt abort keeps disk states."""

from __future__ import annotations

import subprocess
import time


def _procs(marker):
    out = subprocess.run(["ps", "-eo", "args"], capture_output=True, text=True).stdout
    return [l for l in out.splitlines() if marker in l and "ps -eo" not in l]


def test_graceful_shutdown_cancels_running_job_and_reaps(client_factory):
    client, service, _ = client_factory()
    marker = "66001"
    jid = client.post(
        "/jobs",
        json={
            "submitter": "u",
            "argv": [
                "python3",
                "-c",
                f"import subprocess,time;subprocess.Popen(['sleep','{marker}']);time.sleep(30)",
            ],
            "limits": {"wall_time_s": 30},
        },
    ).json()["job_id"]
    time.sleep(0.6)
    assert client.get(f"/jobs/{jid}").json()["state"] == "running"
    service.shutdown()
    time.sleep(0.4)
    # process tree reaped on graceful stop
    assert not _procs(f"sleep {marker}")
    # graceful shutdown records the cancellation outcome
    assert service.store.get(jid).state.value in ("cancelled", "failed")
