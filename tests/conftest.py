import os
import tempfile
import time

import pytest

from sandbox_runtime.config import Settings
from sandbox_runtime.executor import Executor
from sandbox_runtime.models import JobResult
from sandbox_runtime.scheduler import Scheduler
from sandbox_runtime.store import JobStore

NOBODY_UID = 65534
NOBODY_GID = 65534


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        data_dir=str(tmp_path),
        global_concurrency=2,
        per_submitter_concurrency=1,
        drop_uid=NOBODY_UID,
        drop_gid=NOBODY_GID,
    )


@pytest.fixture()
def store(settings):
    s = JobStore(os.path.join(settings.data_dir, "jobs.db"))
    s.open()
    yield s
    s.close()


@pytest.fixture()
def executor(settings):
    return Executor(
        os.path.join(settings.data_dir, "work"),
        drop_uid=settings.drop_uid,
        drop_gid=settings.drop_gid,
    )


@pytest.fixture()
def scheduler(settings, store, executor):
    sch = Scheduler(settings, store, executor)
    sch.start()
    yield sch
    sch.shutdown()


def wait_terminal(scheduler: Scheduler, job_id: str, timeout: float = 30.0) -> JobResult:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = scheduler.get(job_id)
        if result is not None and result.status.terminal:
            return result
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not reach a terminal state")
