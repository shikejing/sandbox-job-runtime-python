"""Isolation: privilege drop, degraded mode observability, env, network."""

import os
import threading

from sandbox_runtime.executor import Executor
from sandbox_runtime.models import IsolationMode, JobSpec, JobStatus

from .conftest import NOBODY_GID, NOBODY_UID, wait_terminal


def test_runs_as_unprivileged_user_when_root(executor):
    if os.geteuid() != 0:
        import pytest

        pytest.skip("privilege drop requires root")
    assert executor.isolation_mode() == IsolationMode.FULL
    r = executor.run(
        "j-uid",
        JobSpec(submitter="t", command=["sh", "-c", "echo $(id -u):$(id -g)"]),
        network_granted=False,
        cancel_event=threading.Event(),
    )
    assert r.status == JobStatus.SUCCEEDED
    assert r.stdout.strip() == f"{NOBODY_UID}:{NOBODY_GID}"


def test_degraded_mode_is_explicit_and_observable(tmp_path):
    # No drop uid/gid configured -> privilege drop impossible -> DEGRADED,
    # and the reason must be visible both in capabilities and per-job notes.
    ex = Executor(str(tmp_path / "work"), drop_uid=None, drop_gid=None)
    assert ex.isolation_mode() == IsolationMode.DEGRADED
    assert any("privilege drop unavailable" in n for n in ex.capability_notes())
    r = ex.run(
        "j-deg",
        JobSpec(submitter="t", command=["echo", "hi"]),
        network_granted=False,
        cancel_event=threading.Event(),
    )
    assert r.status == JobStatus.SUCCEEDED
    assert r.isolation == IsolationMode.DEGRADED
    assert any("privilege drop unavailable" in n for n in r.notes)


def test_environment_is_scrubbed(executor):
    r = executor.run(
        "j-env",
        JobSpec(
            submitter="t",
            command=["sh", "-c", "echo PATH=$PATH; echo V=$MY_VAR; echo LDP=${LD_PRELOAD:-unset}"],
            env={"MY_VAR": "hello", "PATH": "/evil/bin", "LD_PRELOAD": "/evil.so"},
        ),
        network_granted=False,
        cancel_event=threading.Event(),
    )
    assert r.status == JobStatus.SUCCEEDED
    assert "/evil/bin" not in r.stdout  # submitter cannot override PATH
    assert "V=hello" in r.stdout  # harmless vars pass through
    assert "LDP=unset" in r.stdout  # dangerous vars filtered


def test_dedicated_workdir(executor):
    r = executor.run(
        "j-cwd",
        JobSpec(submitter="t", command=["sh", "-c", "pwd; touch myfile; ls"]),
        network_granted=False,
        cancel_event=threading.Event(),
    )
    assert r.status == JobStatus.SUCCEEDED
    assert "j-cwd" in r.stdout.splitlines()[0]
    assert "myfile" in r.stdout


def test_network_denied_by_default(scheduler):
    job = scheduler.submit(
        JobSpec(
            submitter="t",
            command=[
                "python3",
                "-c",
                "import socket; socket.create_connection(('8.8.8.8', 53), 2)",
            ],
        )
    )
    r = wait_terminal(scheduler, job)
    assert r.status == JobStatus.FAILED  # connection impossible inside netns
    assert r.network_granted is False


def test_network_request_subject_to_policy(scheduler, settings):
    assert settings.network_policy_allow is False
    job = scheduler.submit(
        JobSpec(submitter="t", command=["echo", "x"], allow_network=True)
    )
    r = wait_terminal(scheduler, job)
    # submitter asked, but service policy denies -> not granted
    assert r.network_granted is False
