"""Cancellation: queued and running jobs, others unaffected."""

import time

from sandbox_runtime.models import JobSpec, JobStatus

from .conftest import wait_terminal


def test_cancel_queued_job(scheduler):
    blocker = scheduler.submit(JobSpec(submitter="a", command=["sleep", "1"]))
    victim = scheduler.submit(JobSpec(submitter="a", command=["echo", "never"]))
    time.sleep(0.2)
    assert scheduler.get(victim).status == JobStatus.QUEUED
    assert scheduler.cancel(victim) is True
    r = scheduler.get(victim)
    assert r.status == JobStatus.CANCELLED
    assert wait_terminal(scheduler, blocker).status == JobStatus.SUCCEEDED
    # cancelling a terminal job is a no-op
    assert scheduler.cancel(victim) is False


def test_cancel_running_job_fast(scheduler):
    victim = scheduler.submit(JobSpec(submitter="b", command=["sleep", "30"]))
    time.sleep(0.3)
    assert scheduler.get(victim).status == JobStatus.RUNNING
    start = time.monotonic()
    assert scheduler.cancel(victim) is True
    r = wait_terminal(scheduler, victim, timeout=10)
    elapsed = time.monotonic() - start
    assert r.status == JobStatus.CANCELLED
    assert elapsed < 5  # took effect quickly, not after the 30s sleep


def test_cancel_does_not_affect_others(scheduler):
    friend = scheduler.submit(
        JobSpec(submitter="c", command=["sh", "-c", "sleep 0.5; echo alive"])
    )
    victim = scheduler.submit(JobSpec(submitter="d", command=["sleep", "30"]))
    time.sleep(0.3)
    assert scheduler.cancel(victim) is True
    assert wait_terminal(scheduler, victim).status == JobStatus.CANCELLED
    r = wait_terminal(scheduler, friend)
    assert r.status == JobStatus.SUCCEEDED
    assert r.stdout.strip() == "alive"
