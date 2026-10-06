"""Restart recovery: unfinished jobs requeued, finished results preserved."""

import os

from sandbox_runtime.executor import Executor
from sandbox_runtime.models import JobResult, JobSpec, JobStatus
from sandbox_runtime.scheduler import Scheduler
from sandbox_runtime.store import JobStore

from .conftest import wait_terminal


def test_restart_recovers_unfinished_and_preserves_finished(settings, store):
    # simulate a crashed previous process: one queued, one running, one done
    store.insert_queued(
        "q1", JobSpec(submitter="eve", command=["echo", "was-queued"]), store.next_seq()
    )
    store.insert_queued(
        "r1", JobSpec(submitter="eve", command=["echo", "was-running"]), store.next_seq()
    )
    store.transition("r1", JobStatus.RUNNING)
    store.insert_queued(
        "d1", JobSpec(submitter="eve", command=["echo", "CHANGED"]), store.next_seq()
    )
    store.transition("d1", JobStatus.RUNNING)
    store.transition(
        "d1",
        JobStatus.SUCCEEDED,
        JobResult(job_id="d1", status=JobStatus.SUCCEEDED, stdout="original"),
    )
    store.close()

    # "restart": reopen the same database
    store2 = JobStore(os.path.join(settings.data_dir, "jobs.db"))
    store2.open()
    executor2 = Executor(
        os.path.join(settings.data_dir, "work"),
        drop_uid=settings.drop_uid,
        drop_gid=settings.drop_gid,
    )
    try:
        assert sorted(store2.unfinished()) == ["q1", "r1"]
        sch = Scheduler(settings, store2, executor2)
        sch.recover()
        sch.start()
        try:
            rq = wait_terminal(sch, "q1")
            rr = wait_terminal(sch, "r1")
            rd = wait_terminal(sch, "d1")
        finally:
            sch.shutdown()
    finally:
        store2.close()

    assert rq.status == JobStatus.SUCCEEDED and rq.stdout.strip() == "was-queued"
    assert rr.status == JobStatus.SUCCEEDED and rr.stdout.strip() == "was-running"
    # succeeded job was NOT re-executed: original result intact
    assert rd.status == JobStatus.SUCCEEDED and rd.stdout.strip() == "original"


def test_unfinished_never_reported_as_success(settings, store):
    store.insert_queued(
        "x1", JobSpec(submitter="eve", command=["exit", "1"]), store.next_seq()
    )
    store.transition("x1", JobStatus.RUNNING)  # crashed mid-flight
    # before recovery completes, the job must not read as succeeded
    r = store.get("x1")
    assert r.status in (JobStatus.QUEUED, JobStatus.RUNNING)
