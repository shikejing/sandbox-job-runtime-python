"""Sandboxed execution of a single job as an OS process tree.

Execution model (default, local, no external services):

* a fresh, empty working directory per job (``HOME``/``TMPDIR`` point into it)
* a scrubbed environment (``env -i`` plus an allow-list plus submitter vars
  after stripping loader/startup overrides such as ``LD_PRELOAD``)
* PID namespace via ``unshare --pid --fork`` when available, so killing the
  namespace init cannot leave grandchildren behind
* network namespace (``unshare --net``) unless the submitter explicitly
  requested the network *and* service policy permits it
* ``prlimit --cpu`` hard processor-time ceiling
* privilege drop to an unprivileged account via ``setpriv``
* wall clock, RSS (process tree) and output volume enforced by the supervisor
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

from .config import Config
from .isolation import IsolationCapabilities
from .model import FailureReason, IsolationInfo, Job, JobResult

_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
_CLK_TCK = os.sysconf("SC_CLK_TCK")

# Environment variables a submitter is never allowed to inject.
_DENY_ENV = {
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "LD_AUDIT",
    "LD_DEBUG",
    "PYTHONPATH",
    "PYTHONSTARTUP",
    "PYTHONHOME",
    "BASH_ENV",
    "ENV",
    "CDPATH",
    "IFS",
    "HOSTALIASES",
    "PERL5LIB",
    "NODE_OPTIONS",
    "RUBYOPT",
}

_BASE_ENV = {
    "PATH": "/usr/local/bin:/usr/bin:/bin",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "SANDBOX": "1",
}


@dataclass(eq=False)
class _OutputReader(threading.Thread):
    stream: object
    limit: int
    name: str = ""
    chunks: list[bytes] = field(default_factory=list)
    size: int = 0
    truncated: bool = False
    _fh: object = None

    def __post_init__(self) -> None:
        super().__init__(daemon=True, name=f"reader-{self.name}")

    def run(self) -> None:
        total = 0
        try:
            while True:
                buf = self.stream.read(65536)
                if not buf:
                    break
                total += len(buf)
                if total <= self.limit:
                    self.chunks.append(buf)
                else:
                    # Keep draining so the child never blocks on a full pipe,
                    # but retain only the first ``limit`` bytes.
                    self.truncated = True
                    room = self.limit - sum(len(c) for c in self.chunks)
                    if room > 0:
                        self.chunks.append(buf[:room])
        finally:
            with contextlib.suppress(OSError):
                self.stream.close()
        self.size = total

    def data(self) -> bytes:
        return b"".join(self.chunks)


def _parse_stat(pid: int) -> tuple[int, float, int] | None:
    """Return ``(pgrp, cpu_seconds, rss_bytes)`` for *pid*, or None."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as fh:
            raw = fh.read()
        with open(f"/proc/{pid}/statm", "rb") as fh:
            statm = fh.read().split()
    except OSError:
        return None
    rparen = raw.rfind(b")")
    fields = raw[rparen + 2 :].split()
    try:
        pgrp = int(fields[2])  # field 5 overall, index 0 here is field 3
        utime = int(fields[11])  # field 14
        stime = int(fields[12])  # field 15
        rss_pages = int(statm[1])
    except (IndexError, ValueError):
        return None
    return pgrp, (utime + stime) / _CLK_TCK, rss_pages * _PAGE_SIZE


def _tree_stats(pgid: int) -> tuple[float, int]:
    """Sum CPU seconds and RSS bytes over every process in *pgid*."""
    cpu = 0.0
    rss = 0
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        stat = _parse_stat(int(entry))
        if stat is None:
            continue
        pgrp, proc_cpu, proc_rss = stat
        if pgrp == pgid:
            cpu += proc_cpu
            rss += proc_rss
    return cpu, rss


def _signal_name(sig: int) -> str:
    try:
        return signal.Signals(sig).name
    except (ValueError, AttributeError):
        return f"SIG{sig}"


class ProcessRunner:
    """Launches and supervises one job at a time (called by the scheduler)."""

    def __init__(self, config: Config, caps: IsolationCapabilities):
        self.config = config
        self.caps = caps

    # ---- working directory ----------------------------------------------
    def prepare_workdir(self, job: Job) -> str:
        work = os.path.join(self.config.state_dir, "jobs", job.job_id, "work")
        os.makedirs(os.path.join(work, "tmp"), exist_ok=True)
        if self.caps.can_drop_privileges and self.caps.runner_uid is not None:
            # The unprivileged account needs to read/write its own sandbox only.
            os.chmod(work, 0o700)
            os.chown(work, self.caps.runner_uid, self.caps.runner_gid or -1)
            os.chown(
                os.path.join(work, "tmp"),
                self.caps.runner_uid,
                self.caps.runner_gid or -1,
            )
        else:
            os.chmod(work, 0o700)
        return work

    def cleanup_workdir(self, job: Job) -> None:
        if not self.config.cleanup_workdirs:
            return
        work = os.path.join(self.config.state_dir, "jobs", job.job_id, "work")
        shutil.rmtree(work, ignore_errors=True)

    # ---- environment ------------------------------------------------------
    def _build_env(self, job: Job, workdir: str) -> dict[str, str]:
        env = dict(_BASE_ENV)
        env["HOME"] = workdir
        env["TMPDIR"] = os.path.join(workdir, "tmp")
        for key, value in job.env.items():
            key_u = key.upper()
            if key_u in _DENY_ENV or key_u.startswith("LD_"):
                continue
            env[key] = value
        return env

    # ---- command assembly -------------------------------------------------
    def _build_argv(
        self, job: Job, env: dict[str, str], isolate_net: bool
    ) -> tuple[list[str], IsolationInfo]:
        caps = self.caps
        info = IsolationInfo()
        limits = job.limits
        assert limits is not None
        chain: list[str] = []

        # Outer wrapper: namespaces.
        ns_args: list[str] = ["unshare"]
        if caps.can_pid_namespace and shutil.which("unshare"):
            # --fork makes unshare clone a child so nsinit becomes PID 1 of
            # the new namespace (real fatal signals then keep their default
            # disposition).  --kill-child guarantees that whenever *unshare*
            # dies -- even from SIGKILL -- its child (and therefore every
            # process in the namespace) is reaped, leaving no orphans.
            ns_args += ["--pid", "--fork", "--kill-child"]
            info.pid_namespace = True
        else:
            info.degraded = True
            info.degradation_reasons.append("pid_namespace_unavailable")
        if isolate_net:
            if caps.can_net_namespace and shutil.which("unshare"):
                ns_args.append("--net")
                info.net_namespace = True
                info.network_isolated = True
            else:
                info.degraded = True
                info.degradation_reasons.append("network_isolation_unavailable")
        if len(ns_args) > 1 and shutil.which("unshare"):
            chain.extend(ns_args)
            chain.append("--")
            # Our own PID-1 reaper runs the payload so fatal signals keep
            # their default action and grandchildren get reaped.
            if info.pid_namespace:

                chain.extend([sys.executable, "-m", "sandbox._nsinit"])

        # Per-process CPU rlimit: set slightly above the supervisor ceiling so
        # the aggregate monitor normally classifies first; this stays as a
        # hard backstop (SIGXCPU/SIGKILL) for runaway single processes.
        if caps.can_setrlimit and shutil.which("prlimit"):
            chain.extend(["prlimit", f"--cpu={limits.cpu_time_s + 2}"])

        # Privilege drop.
        if caps.can_drop_privileges and caps.runner_uid is not None:
            chain.extend(
                [
                    "setpriv",
                    f"--reuid={caps.runner_uid}",
                    f"--regid={caps.runner_gid}",
                    "--init-groups",
                    "--no-new-privs",
                ]
            )
            info.privilege_dropped = True
        else:
            info.degraded = True
            info.degradation_reasons.append("privilege_drop_unavailable")

        # Scrubbed environment, then the untrusted payload.
        chain.append("env")
        chain.append("-i")
        chain.extend(f"{k}={v}" for k, v in sorted(env.items()))
        chain.extend(job.argv)
        return chain, info

    # ---- main entry -------------------------------------------------------
    def run(self, job: Job) -> JobResult:
        assert job.limits is not None
        started = time.time()
        workdir = self.prepare_workdir(job)
        job.workdir = workdir

        # Network policy: default closed; a request must be explicit and
        # enabled by service policy, and the platform must be able to enforce
        # isolation.  Anything else is refused, never silently run with a NIC.
        isolate_net = not job.request_network
        if job.request_network:
            if not self.config.allow_network or (
                not self.caps.can_net_namespace
                and not self.config.allow_network_on_degraded_isolation
            ):
                return JobResult(
                    failure_reason=FailureReason.NETWORK_POLICY_DENIED.value,
                    stderr=(
                        "network denied by service policy or unavailable network "
                        "isolation; refusing to run with open network access"
                    ),
                    started_at=started,
                    finished_at=time.time(),
                )
            isolate_net = False

        env = self._build_env(job, workdir)
        # nsinit (namespace PID 1, runs before privilege drop) reports the
        # payload's true wait status here. Kept outside the work directory so
        # the unprivileged payload can neither read nor forge it.
        status_path = os.path.join(
            self.config.state_dir, "jobs", job.job_id, ".nsstatus.json"
        )
        with contextlib.suppress(OSError):
            os.unlink(status_path)
        spawn_env = dict(os.environ)
        spawn_env["SANDBOX_NS_STATUS"] = status_path
        # The interpreter running nsinit must find this package even when the
        # service was started from a working directory the unprivileged user
        # cannot traverse.  This only feeds our own init; the payload env is
        # scrubbed afterwards by "env -i".
        import sandbox as _sandbox_pkg

        pkg_root = os.path.dirname(os.path.dirname(os.path.abspath(_sandbox_pkg.__file__)))
        spawn_env["PYTHONPATH"] = (
            pkg_root + (os.pathsep + spawn_env["PYTHONPATH"] if spawn_env.get("PYTHONPATH") else "")
        )
        argv, info = self._build_argv(job, env, isolate_net)
        job.isolation = info

        return self._spawn_and_supervise(
            job, argv, spawn_env, workdir, started, info, status_path
        )

    def _spawn_and_supervise(
        self,
        job: Job,
        argv: list[str],
        spawn_env: dict,
        workdir: str,
        started: float,
        info: IsolationInfo,
        status_path: str,
    ) -> JobResult:
        limits = job.limits
        assert limits is not None
        reason: FailureReason | None = None
        detail = ""
        proc: subprocess.Popen | None = None
        try:
            proc = subprocess.Popen(
                argv,
                cwd=workdir,
                env=spawn_env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                # Always a fresh session: when namespacing, the kill must stay
                # inside this tree; when not, process-group kill still works.
                start_new_session=True,
            )
        except (OSError, ValueError) as exc:
            return JobResult(
                failure_reason=FailureReason.INTERNAL_ERROR.value,
                stderr=f"failed to launch job: {exc!r}",
                started_at=started,
                finished_at=time.time(),
            )

        out = _OutputReader(proc.stdout, limits.output_bytes, name=f"{job.job_id}-out")
        err = _OutputReader(proc.stderr, limits.output_bytes, name=f"{job.job_id}-err")
        out.start()
        err.start()

        deadline = started + limits.wall_time_s
        mem_bytes = limits.memory_mb * 1024 * 1024
        peak_rss = 0
        cpu_seen = 0.0

        while True:
            # Sample first so even sub-poll-interval jobs record usage and
            # get at least one resource-limit check.
            if proc.poll() is None:
                if job.cancel_requested:
                    reason = FailureReason.CANCELLED
                    detail = "cancelled while running"
                    self._kill_tree(proc, info.pid_namespace)
                    rc = proc.wait()
                    break

                now = time.time()
                if now >= deadline:
                    reason = FailureReason.WALL_TIMEOUT
                    detail = f"wall time limit {limits.wall_time_s}s exceeded"
                    self._kill_tree(proc, info.pid_namespace)
                    rc = proc.wait()
                    break

                with contextlib.suppress(OSError):
                    pgid = os.getpgid(proc.pid)
                    cpu_seen, rss = _tree_stats(pgid)
                    peak_rss = max(peak_rss, rss)
                    if cpu_seen >= limits.cpu_time_s:
                        reason = FailureReason.CPU_TIME_EXCEEDED
                        detail = f"cpu time limit {limits.cpu_time_s}s exceeded"
                        self._kill_tree(proc, info.pid_namespace)
                        rc = proc.wait()
                        break
                    if rss >= mem_bytes:
                        reason = FailureReason.MEMORY_EXCEEDED
                        detail = f"memory limit {limits.memory_mb}MB exceeded"
                        self._kill_tree(proc, info.pid_namespace)
                        rc = proc.wait()
                        break

                if out.truncated or err.truncated:
                    reason = FailureReason.OUTPUT_EXCEEDED
                    detail = f"output limit {limits.output_bytes} bytes exceeded"
                    self._kill_tree(proc, info.pid_namespace)
                    rc = proc.wait()
                    break

            try:
                rc = proc.wait(timeout=self.config.poll_interval_s)
                break
            except subprocess.TimeoutExpired:
                continue

        out.join(timeout=5)
        err.join(timeout=5)
        finished = time.time()

        # A fast-exiting job can finish before the output readers flagged the
        # overflow; attribute after draining the pipes so output volume is
        # enforced even when the process beat the polling loop.
        if reason is None and (out.truncated or err.truncated):
            reason = FailureReason.OUTPUT_EXCEEDED
            detail = f"output limit {limits.output_bytes} bytes exceeded"

        # Final usage sample (tree may be gone; ignore errors).
        with contextlib.suppress(OSError):
            pgid = os.getpgid(proc.pid)
            cpu_seen, rss = _tree_stats(pgid)
            peak_rss = max(peak_rss, rss)

        # Recover the payload's true wait status from the namespace init.
        ns_signal: int | None = None
        with contextlib.suppress(OSError, ValueError):
            with open(status_path, "r", encoding="utf-8") as fh:
                ns_status = json.load(fh)
            if ns_status.get("kind") == "signal":
                ns_signal = int(ns_status["signal"])
        with contextlib.suppress(OSError):
            os.unlink(status_path)

        result = JobResult(
            failure_reason=reason.value if reason else None,
            started_at=started,
            finished_at=finished,
            peak_rss_mb=round(peak_rss / (1024 * 1024), 3),
            cpu_time_s=round(cpu_seen, 3),
            wall_time_s=round(finished - started, 3),
            stdout=out.data().decode("utf-8", "replace"),
            stderr=(err.data().decode("utf-8", "replace") + (("\n" + detail) if detail else ""))
            if reason
            else err.data().decode("utf-8", "replace"),
            stdout_truncated=out.truncated,
            stderr_truncated=err.truncated,
            timed_out=reason in (FailureReason.WALL_TIMEOUT, FailureReason.CPU_TIME_EXCEEDED),
        )

        if reason is None:
            # Post-hoc attribution: the payload died from a signal the
            # supervisor did not raise.  Use observed usage first, then the
            # known CPU rlimit signal, before falling back to "signal".
            if ns_signal is not None and cpu_seen >= limits.cpu_time_s * 0.9:
                result.signal = ns_signal
                result.failure_reason = FailureReason.CPU_TIME_EXCEEDED.value
                result.stderr = result.stderr + (
                    f"\n{_signal_name(ns_signal)} after {cpu_seen:.2f}s cpu"
                )
            elif ns_signal is not None and peak_rss >= mem_bytes * 0.9:
                result.signal = ns_signal
                result.failure_reason = FailureReason.MEMORY_EXCEEDED.value
                result.stderr = result.stderr + (
                    f"\n{_signal_name(ns_signal)} at {peak_rss // (1024*1024)}MB rss"
                )
            elif ns_signal == signal.SIGXCPU:
                result.signal = ns_signal
                result.failure_reason = FailureReason.CPU_TIME_EXCEEDED.value
                result.stderr = result.stderr + f"\n{_signal_name(ns_signal)}: cpu rlimit hit"
            elif ns_signal is not None:
                # Payload died from a signal the supervisor did not raise.
                result.signal = ns_signal
                result.failure_reason = FailureReason.SIGNAL_TERMINATED.value
                result.stderr = (
                    result.stderr
                    + f"\nterminated by signal {_signal_name(ns_signal)}"
                )
            elif rc == 0:
                result.exit_code = 0
            elif rc > 0:
                result.exit_code = rc
                result.failure_reason = FailureReason.NONZERO_EXIT.value
            elif rc < 0:
                result.signal = -rc
                result.failure_reason = FailureReason.SIGNAL_TERMINATED.value
                result.stderr = (
                    result.stderr + f"\nterminated by signal {_signal_name(-rc)}"
                )
        else:
            result.exit_code = rc if rc is not None and rc >= 0 else None
            result.signal = (
                ns_signal if ns_signal is not None
                else (-rc if rc is not None and rc < 0 else None)
            )

        self._ensure_reaped(proc, info.pid_namespace)
        return result

    # ---- process tree teardown -------------------------------------------
    def _kill_tree(self, proc: subprocess.Popen, pid_ns: bool) -> None:
        """Terminate the whole tree; escalate to SIGKILL; leave nothing behind."""
        if pid_ns:
            # The namespace init (nsinit) is session leader; SIGTERM is
            # forwarded to the payload, then SIGKILLing init destroys the
            # entire PID namespace, guaranteeing zero survivors.
            with contextlib.suppress(ProcessLookupError, OSError):
                os.kill(proc.pid, signal.SIGTERM)
            time.sleep(0.1)
            with contextlib.suppress(ProcessLookupError, OSError):
                os.kill(proc.pid, signal.SIGKILL)
        else:
            with contextlib.suppress(ProcessLookupError, OSError):
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGTERM)
            time.sleep(0.1)
            with contextlib.suppress(ProcessLookupError, OSError):
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGKILL)
        with contextlib.suppress(OSError):
            proc.wait(timeout=3)
        self._ensure_reaped(proc, pid_ns)

    def _ensure_reaped(self, proc: subprocess.Popen, pid_ns: bool = False) -> None:
        with contextlib.suppress(OSError):
            proc.wait(timeout=2)
        if not pid_ns:
            # No namespace: verify nothing sharing the job's pgrp survives.
            with contextlib.suppress(OSError):
                pgid = os.getpgid(proc.pid)
                survivors = [
                    int(p)
                    for p in os.listdir("/proc")
                    if p.isdigit() and (_parse_stat(int(p)) or (-1, 0, 0))[0] == pgid
                ]
                for pid in survivors:
                    with contextlib.suppress(ProcessLookupError, OSError):
                        os.kill(pid, signal.SIGKILL)
