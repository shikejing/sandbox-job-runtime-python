"""Isolation, privilege drop, env scrubbing, workdir and crash classification."""

from __future__ import annotations

from fastapi.testclient import TestClient

from .conftest import wait_state


def _run(client, argv, **kw):
    body = {"submitter": "u", "argv": argv, **kw}
    r = client.post("/jobs", json=body)
    assert r.status_code == 201, r.text
    return wait_state(client, r.json()["job_id"], timeout=30)


def test_runs_unprivileged_when_capable(client: TestClient, caps):
    if not caps.can_drop_privileges:
        import pytest

        pytest.skip("privilege drop unavailable on this host")
    job = _run(client, ["python3", "-c", "import os;print(os.geteuid())"])
    assert job["state"] == "succeeded"
    assert int(job["result"]["stdout"].strip()) != 0
    assert job["isolation"]["privilege_dropped"] is True


def test_pid_and_network_namespace_used(client: TestClient, caps):
    job = _run(client, ["true"])
    iso = job["isolation"]
    if caps.can_pid_namespace:
        assert iso["pid_namespace"] is True
    else:
        assert iso["degraded"] is True
        assert "pid_namespace_unavailable" in iso["degradation_reasons"]
    # Default network posture: a fresh, empty network namespace.
    assert iso["network_isolated"] is True


def test_scrubbed_environment_and_private_workdir(client: TestClient):
    job = _run(
        client,
        [
            "python3",
            "-c",
            (
                "import os,json\n"
                "keys=[k for k in os.environ if k.startswith(('LD_','PYTHON'))]\n"
                "print(json.dumps({'bad':keys,'home':os.environ.get('HOME'),"
                "'cwd':os.getcwd(),'var':os.environ.get('MY_VAR')}))"
            ),
        ],
        env={"LD_PRELOAD": "/evil.so", "PYTHONPATH": "/evil", "MY_VAR": "42"},
    )
    import json

    info = json.loads(job["result"]["stdout"])
    assert info["bad"] == []  # loader/python overrides stripped
    assert info["var"] == "42"  # benign custom env kept
    # HOME points at the per-job work directory.
    assert job["job_id"] in info["home"]
    assert job["job_id"] in info["cwd"]


def test_nonzero_exit_classified(client: TestClient):
    job = _run(client, ["python3", "-c", "import sys;sys.exit(7)"])
    assert job["state"] == "failed"
    assert job["result"]["failure_reason"] == "nonzero_exit"
    assert job["result"]["exit_code"] == 7


def test_fatal_signal_classified(client: TestClient):
    job = _run(
        client,
        ["python3", "-c", "import os,signal;os.kill(os.getpid(),signal.SIGSEGV)"],
    )
    assert job["state"] == "failed"
    assert job["result"]["failure_reason"] == "signal_terminated"
    assert job["result"]["signal"] == 11


def test_hardware_exception_classified(client: TestClient):
    job = _run(client, ["python3", "-c", "import ctypes;ctypes.string_at(0)"])
    assert job["state"] == "failed"
    assert job["result"]["failure_reason"] == "signal_terminated"
    assert job["result"]["signal"] in (6, 11)


def test_network_request_denied_by_default_policy(client: TestClient):
    r = client.post(
        "/jobs",
        json={"submitter": "u", "argv": ["true"], "request_network": True},
    )
    assert r.status_code == 403
    detail = r.json()["detail"]
    assert detail["error"] == "network_policy_denied"


def test_default_job_has_no_network(client: TestClient):
    job = _run(
        client,
        [
            "python3",
            "-c",
            (
                "import socket\n"
                "s=socket.socket();s.settimeout(2)\n"
                "try:\n s.connect(('1.1.1.1',53));print('OPEN')\n"
                "except OSError: print('CLOSED')"
            ),
        ],
    )
    assert job["state"] == "succeeded"
    assert "CLOSED" in job["result"]["stdout"]


def test_isolation_metadata_always_present_and_honest(client: TestClient, caps):
    job = _run(client, ["true"])
    iso = job["isolation"]
    assert set(
        [
            "pid_namespace",
            "net_namespace",
            "network_isolated",
            "privilege_dropped",
            "degraded",
            "degradation_reasons",
        ]
    ).issubset(iso)
    if not caps.can_pid_namespace:
        assert iso["pid_namespace"] is False and iso["degraded"] is True
