# Isolation model, privilege strategy and guarantees

This service runs **untrusted** commands submitted over a small HTTP API and
reports the true outcome. The default path works on a single Linux machine and
needs no external service, container runtime or network.

## 1. Process and workload isolation

Each job executes through the following wrapper chain (outermost first):

```
unshare --pid --fork --kill-child [--net] --
  <python> -m sandbox._nsinit        # PID 1 inside the new PID namespace
    prlimit --cpu=<cpu+2>            # hard per-process processor-time backstop
      setpriv --reuid=<uid> --regid=<gid> --init-groups --no-new-privs
        env -i <allow-list...>       # scrubbed environment
          <payload argv>
```

* **Private working directory.** Every job gets a fresh, mode-`0700` directory
  (`<state_dir>/jobs/<id>/work`) as its `cwd`, `HOME` and `TMPDIR`. The
  directory is chowned to the unprivileged account and removed on completion
  (`SANDBOX_CLEANUP_WORKDIRS`).
* **PID namespace + minimal init.** `unshare --pid --fork` places the job in a
  new PID namespace. A small bundled init (`sandbox/_nsinit.py`) acts as PID 1:
  it forks the payload, forwards termination signals, reaps grandchildren and
  records the payload's real wait status. This matters because a hand-rolled
  payload as PID 1 would swallow fatal default signals.
* **No orphaned processes.** `unshare --kill-child` guarantees that if the
  `unshare` process dies for *any* reason (including `SIGKILL`), its child and
  every process in the namespace is terminated. The supervisor additionally
  kills by process group in degraded (non-namespaced) mode and verifies no
  process with the job's group id survives.
* **Network namespace.** Unless network access is explicitly requested *and*
  permitted, the job runs under `unshare --net` with only the loopback device;
  it has no route to any external address.
* **No root.** `setpriv` drops to an unprivileged uid/gid, initializes a minimal
  group set and sets `NO_NEW_PRIVS` so setuid binaries cannot re-escalate.
* **Scrubbed environment.** `env -i` starts from a tiny allow-list
  (`PATH`, `LANG`, `LC_ALL`, `SANDBOX`, per-job `HOME`/`TMPDIR`) plus the
  submitter's variables after stripping dangerous keys: `LD_*` loader hooks,
  `PYTHON*` startup hooks, `BASH_ENV`, `ENV`, `IFS`, `NODE_OPTIONS`, etc.

## 2. Network policy

* The default network posture is **closed**.
* A job must set `request_network: true`, **and** the operator must set
  `SANDBOX_ALLOW_NETWORK=true`, for network access to even be considered.
* Even then, the host must be able to provide a network namespace. If it
  cannot, the request is rejected (`403 network_policy_denied`) unless the
  operator additionally sets
  `SANDBOX_ALLOW_NETWORK_ON_DEGRADED_ISOLATION=true`; when that degraded path
  is taken the job's `isolation` record explicitly reports
  `network_isolation_unavailable` and `degraded=true`. There is no code path
  that silently runs an untrusted job with open network access.

## 3. Privilege-drop strategy and observable degradation

Capabilities are **probed with real operations at startup**, never assumed:

* can a child enter a new PID namespace (`unshare --pid`)?
* can a child enter a new network namespace (`unshare --net`)?
* is `setrlimit(RLIMIT_CPU)` permitted?
* is there an unprivileged account to drop to?

When running as root the service creates/uses a dedicated system account
`sandboxrunner` (falling back to `nobody`/`nogroup`/`daemon`), or the account
named by `SANDBOX_RUNNER_USER`. When privilege drop is not possible the job
still runs, but its `isolation` block sets `degraded=true` and lists exactly
which capability is missing (`privilege_drop_unavailable`,
`pid_namespace_unavailable`, `net_namespace_unavailable`). The same honest
report is available from `GET /capabilities` and `GET /health`. Degradation is
therefore always **explicit and observable**, never a silent fallback to a
weaker boundary.

> Note on filesystem isolation: the default boundary uses a private directory,
> dropped privileges and `NO_NEW_PRIVS` rather than a mount namespace/chroot, so
> it remains reproducible on ordinary hosts (including restricted containers).
> An unprivileged job cannot write outside its mode-`0700` working directory
> under the dropped account, but the process is not confined to a private mount
> tree. Treat this as a strong lightweight sandbox, not a virtual-machine
> boundary. Run the service as a non-root user inside a dedicated container if
> you require an even stronger default.

## 4. Resource limits and distinct failure reasons

| Dimension | Enforcement | Failure reason |
| --- | --- | --- |
| Wall clock | supervisor deadline (`SIGTERM`→`SIGKILL`) | `wall_timeout` |
| Processor time | aggregate tree sampling **and** `prlimit --cpu` backstop | `cpu_time_exceeded` |
| Memory | supervisor samples summed tree RSS and kills at the ceiling | `memory_exceeded` |
| Output volume | bounded readers drain stdout/stderr but retain only the cap | `output_exceeded` |
| Non-zero exit | payload exits with code ≠ 0 | `nonzero_exit` |
| Fatal signal | payload terminated by a signal (true status via nsinit) | `signal_terminated` |
| Cancellation | submitter cancel (queued or running) | `cancelled` |
| Network policy | request denied by policy/capability | `network_policy_denied` |
| Internal error | unexpected scheduler/runner exception | `internal_error` |
| Restart interruption | job was queued/running when a previous process died | `interrupted_by_restart` |

Every limit violation terminates the whole process tree (namespace kill or
process-group kill) and is verified to leave no survivors. Peak RSS, CPU and
wall time are returned in the result for observability.

## 5. State machine and persistence

```
queued ──► running ──► succeeded
   │           │
   ├──────────►├──► failed
   └──────────►└──► cancelled
```

* States are monotonic: terminal states never change. Transitions are checked
  centrally and increment a persisted `state_version`.
* Each state is written to `<state_dir>/jobs/<id>/state.json` using
  temp-file + `fsync` + atomic rename, so a crash can only expose the previous
  complete state or the next complete state — never a torn document.
* Every query returns one complete committed state.

## 6. Scheduling and starvation

A single dispatch thread scans the queue in FCFS order on each tick. Admission
requires a free global slot *and* a free per-submitter slot. Every **admissible**
job is launched each tick; a saturated submitter is skipped rather than
head-of-line blocking later submitters. Excess submissions stay queued (HTTP
201, state `queued`) and are never rejected at submission.

## 7. Cancellation

* A queued job is finalized atomically at the dispatch point (`cancelled`,
  `started_at=null` — it never executed).
* A running job receives `SIGTERM` then `SIGKILL`; the whole namespace/group is
  torn down and its working directory removed. Other jobs are unaffected.

## 8. Restart recovery

On startup the store loads all job files.

* **Terminal jobs** (succeeded/failed/cancelled) are loaded as facts and their
  stored results returned verbatim — they are **never re-executed**.
* **Unfinished jobs** (queued/running in a dead process) are reconciled to
  terminal `failed` with the distinct reason `interrupted_by_restart`
  (`running` uses a recovery-only transition, because that execution can never
  resume or be observed). They are never reported as successful and may simply
  be resubmitted.
* `GET /health` reports `recovered_finished` and `recovered_interrupted`
  counters after each boot.

## 9. HTTP surface

| Method & path | Purpose |
| --- | --- |
| `POST /jobs` | Submit a job (201, starts in `queued`). |
| `GET /jobs?submitter=` | List jobs (optionally per submitter). |
| `GET /jobs/{id}` | Fetch one complete committed state incl. result/output. |
| `POST /jobs/{id}/cancel` | Request cancellation (202). |
| `GET /capabilities` | Honest isolation capability and policy report. |
| `GET /health` | Liveness plus recovery/degradation summary. |
