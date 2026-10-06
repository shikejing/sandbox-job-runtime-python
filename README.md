# sandbox-job-runtime (Python)

A small, dependency-light service that accepts **untrusted** command jobs over
HTTP, runs them locally under strong, observable isolation with per-job
resource limits, and reports the true result and failure cause. The default
path is fully reproducible on a single Linux machine and requires **no external
services**.

## Quick start

```bash
uv sync
uv run uvicorn sandbox.app:app --host 127.0.0.1 --port 8000
# interactive docs at http://127.0.0.1:8000/docs
```

Run the tests:

```bash
uv run pytest -q
```

## Example

```bash
# submit (network is closed by default, jobs run unprivileged)
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "submitter": "alice",
  "argv": ["python3", "-c", "print(2**10)"],
  "limits": {"wall_time_s": 10, "cpu_time_s": 5, "memory_mb": 128, "output_bytes": 65536}
}'

# poll
curl -s localhost:8000/jobs/<job_id>

# cancel
curl -s -X POST localhost:8000/jobs/<job_id>/cancel

# inspect the actual isolation strength of this host
curl -s localhost:8000/capabilities
```

### States

`queued -> running -> succeeded | failed | cancelled` (monotonic, versioned,
atomically persisted).

### Failure reasons

`wall_timeout`, `cpu_time_exceeded`, `memory_exceeded`, `output_exceeded`,
`nonzero_exit`, `signal_terminated`, `cancelled`, `network_policy_denied`,
`internal_error`, `interrupted_by_restart`.

## Guarantees at a glance

- **Submission & querying** with queued/running/succeeded/failed/cancelled,
  monotonic versioned state; every read is one complete committed state.
- **Concurrency control** with global and per-submitter caps; overflow queues
  (never rejects), FCFS, and a long job never starves other submitters.
- **Resource limits** across wall time, CPU time, tree RSS and output volume,
  each with a distinct reason; violating processes are fully killed.
- **Isolation & least privilege**: private per-job working directory, scrubbed
  env, PID + network namespaces, unprivileged account with `NO_NEW_PRIVS`.
  Network is closed unless explicitly requested **and** policy-enabled.
- **Cancellation** of queued or running jobs without affecting others, with
  full process-tree and working-directory reclamation.
- **Crash isolation**: non-zero exits, fatal signals, timeouts and unknown
  errors are classified and never take down the scheduler.
- **Restart recovery**: completed jobs are returned verbatim and never re-run;
  interrupted jobs are distinctly marked (never reported as success).

## Documentation

- [docs/isolation.md](docs/isolation.md) — isolation boundary, privilege-drop
  strategy, degradation behavior, scheduling and recovery.
- [docs/configuration.md](docs/configuration.md) — every configuration knob and
  the submit payload schema.

## Layout

```
sandbox/
  config.py      env configuration
  model.py       states, failure reasons, limits, Job entity
  storage.py     atomic per-job persistence + recovery loading
  isolation.py   real capability probing + unprivileged user setup
  _nsinit.py     PID-1 reaper inside the job PID namespace
  runner.py      process launch and resource supervision
  scheduler.py   FCFS dispatch + concurrency caps + worker isolation
  service.py     lifecycle + restart reconciliation
  api.py         HTTP routes
  app.py         ASGI factory
tests/           automated coverage of every guarantee above
```
