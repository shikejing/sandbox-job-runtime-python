"""Each resource dimension breaches with a distinguishable reason."""

import threading

import pytest

from sandbox_runtime.models import FailureKind, JobSpec, JobStatus, ResourceLimits

from .conftest import wait_terminal


def test_wall_timeout(scheduler):
    job = scheduler.submit(
        JobSpec(
            submitter="t",
            command=["sleep", "30"],
            limits=ResourceLimits(wall_timeout_s=0.5),
        )
    )
    r = wait_terminal(scheduler, job)
    assert r.status == JobStatus.FAILED
    assert r.failure_kind == FailureKind.WALL_TIMEOUT


def test_cpu_time_limit(scheduler):
    job = scheduler.submit(
        JobSpec(
            submitter="t",
            command=["sh", "-c", "while :; do :; done"],
            limits=ResourceLimits(cpu_time_s=1, wall_timeout_s=30),
        )
    )
    r = wait_terminal(scheduler, job)
    assert r.status == JobStatus.FAILED
    assert r.failure_kind == FailureKind.CPU_LIMIT


def test_memory_limit(scheduler):
    grow = (
        "x=bytearray()\nimport time\n"
        "for _ in range(50):\n x += bytearray(10*1024*1024)\n time.sleep(0.1)"
    )
    job = scheduler.submit(
        JobSpec(
            submitter="t",
            command=["python3", "-c", grow],
            limits=ResourceLimits(
                memory_bytes=128 * 1024 * 1024, wall_timeout_s=30
            ),
        )
    )
    r = wait_terminal(scheduler, job)
    assert r.status == JobStatus.FAILED
    assert r.failure_kind == FailureKind.MEMORY_LIMIT


def test_output_limit(scheduler):
    job = scheduler.submit(
        JobSpec(
            submitter="t",
            command=["sh", "-c", "yes | head -c 10000000"],
            limits=ResourceLimits(max_output_bytes=100_000),
        )
    )
    r = wait_terminal(scheduler, job)
    assert r.status == JobStatus.FAILED
    assert r.failure_kind == FailureKind.OUTPUT_LIMIT
    assert len(r.stdout) <= 100_000


def test_no_leftover_processes_after_kill(scheduler):
    job = scheduler.submit(
        JobSpec(
            submitter="t",
            command=["sh", "-c", "sleep 300 & sleep 300"],
            limits=ResourceLimits(wall_timeout_s=0.5),
        )
    )
    r = wait_terminal(scheduler, job)
    assert r.failure_kind == FailureKind.WALL_TIMEOUT
    import subprocess

    out = subprocess.run(["pgrep", "-x", "sleep"], capture_output=True, text=True)
    survivors = [p for p in out.stdout.split() if p.strip()]
    assert survivors == [], f"leftover processes: {survivors}"
