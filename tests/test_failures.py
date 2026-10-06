"""Failure classification and crash isolation."""

from sandbox_runtime.models import FailureKind, JobSpec, JobStatus

from .conftest import wait_terminal


def test_non_zero_exit(scheduler):
    job = scheduler.submit(JobSpec(submitter="t", command=["sh", "-c", "exit 3"]))
    r = wait_terminal(scheduler, job)
    assert r.status == JobStatus.FAILED
    assert r.failure_kind == FailureKind.NON_ZERO_EXIT
    assert r.exit_code == 3


def test_signalled(scheduler):
    job = scheduler.submit(
        JobSpec(submitter="t", command=["sh", "-c", "kill -SEGV $$"])
    )
    r = wait_terminal(scheduler, job)
    assert r.status == JobStatus.FAILED
    assert r.failure_kind == FailureKind.SIGNALLED
    assert r.signal == 11


def test_spawn_error(scheduler):
    job = scheduler.submit(JobSpec(submitter="t", command=["/nonexistent/binary"]))
    r = wait_terminal(scheduler, job)
    assert r.status == JobStatus.FAILED
    assert r.failure_kind == FailureKind.SPAWN_ERROR
    assert r.error


def test_failing_job_does_not_break_scheduler(scheduler):
    bad = scheduler.submit(JobSpec(submitter="t", command=["sh", "-c", "exit 1"]))
    good = scheduler.submit(JobSpec(submitter="u", command=["echo", "fine"]))
    assert wait_terminal(scheduler, bad).status == JobStatus.FAILED
    r = wait_terminal(scheduler, good)
    assert r.status == JobStatus.SUCCEEDED
    assert r.stdout.strip() == "fine"
    # scheduler still accepts work afterwards
    later = scheduler.submit(JobSpec(submitter="v", command=["echo", "later"]))
    assert wait_terminal(scheduler, later).status == JobStatus.SUCCEEDED


def test_success_result_fields(scheduler):
    job = scheduler.submit(
        JobSpec(submitter="t", command=["sh", "-c", "echo out; echo err 1>&2"])
    )
    r = wait_terminal(scheduler, job)
    assert r.status == JobStatus.SUCCEEDED
    assert r.exit_code == 0
    assert r.stdout.strip() == "out"
    assert r.stderr.strip() == "err"
