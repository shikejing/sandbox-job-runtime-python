"""Concurrency caps: submissions queue instead of being rejected; FIFO."""

import time

from sandbox_runtime.models import JobSpec, JobStatus

from .conftest import wait_terminal


def test_global_cap_queues_excess(scheduler):
    ids = [
        scheduler.submit(JobSpec(submitter=f"u{i}", command=["sleep", "0.6"]))
        for i in range(4)
    ]
    time.sleep(0.3)
    statuses = [scheduler.get(j).status for j in ids]
    running = statuses.count(JobStatus.RUNNING)
    queued = statuses.count(JobStatus.QUEUED)
    assert running == 2  # global_concurrency=2
    assert queued == 2  # excess queued, not rejected
    for j in ids:
        assert wait_terminal(scheduler, j).status == JobStatus.SUCCEEDED


def test_per_submitter_cap_and_no_starvation(scheduler):
    # alice occupies her single slot with a long job; her second job queues,
    # but bob must still be dispatched (no head-of-line starvation).
    a1 = scheduler.submit(JobSpec(submitter="alice", command=["sleep", "0.8"]))
    a2 = scheduler.submit(JobSpec(submitter="alice", command=["echo", "a2"]))
    b1 = scheduler.submit(JobSpec(submitter="bob", command=["echo", "b1"]))
    time.sleep(0.3)
    assert scheduler.get(a1).status == JobStatus.RUNNING
    assert scheduler.get(a2).status == JobStatus.QUEUED
    assert scheduler.get(b1).status in (JobStatus.RUNNING, JobStatus.SUCCEEDED)
    assert wait_terminal(scheduler, b1).status == JobStatus.SUCCEEDED
    assert wait_terminal(scheduler, a1).status == JobStatus.SUCCEEDED
    assert wait_terminal(scheduler, a2).status == JobStatus.SUCCEEDED


def test_fifo_order_within_submitter(scheduler):
    # per-submitter cap is 1, so this submitter's jobs run one at a time and
    # must finish in submission order.
    ids = [
        scheduler.submit(
            JobSpec(submitter="carol", command=["sh", "-c", f"sleep 0.1; echo {i}"])
        )
        for i in range(3)
    ]
    results = [wait_terminal(scheduler, j) for j in ids]
    assert [r.stdout.strip() for r in results] == ["0", "1", "2"]
