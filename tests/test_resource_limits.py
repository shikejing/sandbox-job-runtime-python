"""Each resource dimension terminates the job with a distinct reason."""

from __future__ import annotations

from fastapi.testclient import TestClient

from .conftest import wait_state


def _run_with_limits(client, argv, **limits):
    body = {"submitter": "u", "argv": argv, "limits": limits}
    r = client.post("/jobs", json=body)
    assert r.status_code == 201, r.text
    return wait_state(client, r.json()["job_id"], timeout=30)


def test_wall_clock_timeout(client: TestClient):
    job = _run_with_limits(
        client,
        ["python3", "-c", "import time;time.sleep(30)"],
        wall_time_s=1,
        cpu_time_s=20,
    )
    assert job["state"] == "failed"
    assert job["result"]["failure_reason"] == "wall_timeout"
    assert job["result"]["signal"] == 9
    assert job["result"]["wall_time_s"] < 5


def test_cpu_time_limit(client: TestClient):
    job = _run_with_limits(
        client,
        ["python3", "-c", "x=0\nwhile True: x+=1"],
        wall_time_s=20,
        cpu_time_s=1,
    )
    assert job["state"] == "failed"
    assert job["result"]["failure_reason"] == "cpu_time_exceeded"
    assert job["result"]["cpu_time_s"] >= 0.9


def test_memory_limit(client: TestClient):
    job = _run_with_limits(
        client,
        [
            "python3",
            "-c",
            "x=bytearray(150*1024*1024)\nimport time\ntime.sleep(10)",
        ],
        wall_time_s=20,
        cpu_time_s=20,
        memory_mb=100,
    )
    assert job["state"] == "failed"
    assert job["result"]["failure_reason"] == "memory_exceeded"
    assert job["result"]["peak_rss_mb"] >= 90


def test_output_limit(client: TestClient):
    job = _run_with_limits(
        client,
        ["python3", "-c", "print('x'*100000)"],
        output_bytes=1000,
        wall_time_s=10,
        cpu_time_s=10,
    )
    assert job["state"] == "failed"
    assert job["result"]["failure_reason"] == "output_exceeded"
    # retained output never exceeds the cap
    assert len(job["result"]["stdout"]) <= 1000


def test_reasons_are_distinguishable(client: TestClient):
    wall = _run_with_limits(
        client, ["python3", "-c", "import time;time.sleep(30)"], wall_time_s=1, cpu_time_s=20
    )["result"]["failure_reason"]
    cpu = _run_with_limits(
        client, ["python3", "-c", "while True: pass"], wall_time_s=20, cpu_time_s=1
    )["result"]["failure_reason"]
    mem = _run_with_limits(
        client,
        ["python3", "-c", "bytearray(150*1024*1024)"],
        memory_mb=50,
        wall_time_s=10,
        cpu_time_s=10,
    )["result"]["failure_reason"]
    out = _run_with_limits(
        client, ["python3", "-c", "print('y'*50000)"], output_bytes=100, wall_time_s=10, cpu_time_s=10
    )["result"]["failure_reason"]
    assert len({wall, cpu, mem, out}) == 4


def test_limits_do_not_harm_normal_job(client: TestClient):
    job = _run_with_limits(
        client,
        ["python3", "-c", "print('fine')"],
        wall_time_s=10,
        cpu_time_s=10,
        memory_mb=128,
        output_bytes=65536,
    )
    assert job["state"] == "succeeded"
