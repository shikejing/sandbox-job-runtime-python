# Configuration

All configuration is read from environment variables. Every value has a safe
default, so the service runs on a single local machine with **no external
dependencies**.

| Variable | Default | Meaning |
| --- | --- | --- |
| `SANDBOX_STATE_DIR` | `/tmp/sandbox-jobs` | Directory for per-job state, outputs and working dirs. |
| `SANDBOX_MAX_GLOBAL_CONCURRENCY` | `4` | At most this many jobs running at once, across all submitters. |
| `SANDBOX_MAX_PER_SUBMITTER_CONCURRENCY` | `2` | Per-submitter concurrency cap. |
| `SANDBOX_DEFAULT_WALL_TIME_S` | `30` | Default wall-clock limit (seconds) when the submitter omits it. |
| `SANDBOX_DEFAULT_CPU_TIME_S` | `10` | Default processor-time limit (seconds, summed over the process tree). |
| `SANDBOX_DEFAULT_MEMORY_MB` | `256` | Default resident-memory limit (MiB, summed over the process tree). |
| `SANDBOX_DEFAULT_OUTPUT_BYTES` | `1048576` | Default combined-ish per-stream stdout/stderr retention cap. |
| `SANDBOX_MAX_WALL_TIME_S` | `600` | Hard ceiling a submitter cannot exceed. |
| `SANDBOX_MAX_CPU_TIME_S` | `300` | Hard ceiling for CPU time. |
| `SANDBOX_MAX_MEMORY_MB` | `2048` | Hard ceiling for memory. |
| `SANDBOX_MAX_OUTPUT_BYTES` | `16777216` | Hard ceiling for output. |
| `SANDBOX_CLEANUP_WORKDIRS` | `true` | Delete each job's working directory after it finishes. |
| `SANDBOX_SCHEDULER_TICK_MS` | `50` | Dispatch loop period. |
| `SANDBOX_POLL_INTERVAL_MS` | `50` | Per-job supervisor sampling interval. |
| `SANDBOX_ALLOW_NETWORK` | `false` | Master switch for the request-network feature. Default **deny**. |
| `SANDBOX_ALLOW_NETWORK_ON_DEGRADED_ISOLATION` | `false` | Permit network requests even when no network namespace is available (still flagged degraded). |
| `SANDBOX_RUNNER_USER` | _(unset)_ | Preferred unprivileged account name. Unset = auto-create `sandboxrunner`, else fall back to `nobody`. |
| `SANDBOX_LOG_LEVEL` | `INFO` | Logging level. |
| `SANDBOX_SKIP_NS_PROBE` | _(unset)_ | Reserved; namespace capability is always probed with a real child. |

## Resource limits in submit payloads

A job may request limits up to the hard ceilings; larger requests are clamped:

```json
{
  "submitter": "alice",
  "argv": ["python3", "-c", "print('hi')"],
  "request_network": false,
  "env": {"MY_VAR": "42"},
  "limits": {
    "wall_time_s": 10,
    "cpu_time_s": 5,
    "memory_mb": 128,
    "output_bytes": 65536
  }
}
```

## Running

```bash
# isolated virtualenv, local only
uv sync
uv run uvicorn sandbox.app:app --host 127.0.0.1 --port 8000
# or
python main.py
```
