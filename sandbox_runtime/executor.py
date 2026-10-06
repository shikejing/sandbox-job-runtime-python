"""Isolated subprocess execution.

Isolation layers (local single machine, no external services), built as a
wrapper chain so privileges are dropped *after* the network namespace is
created and rlimits are applied::

    [unshare --net --] [prlimit ... --] [setpriv ... --] <command>

- dedicated working directory per job, removed after the run
- scrubbed environment (minimal PATH/HOME/TMPDIR + filtered submitter vars)
- new process group so the whole tree can be killed reliably
- rlimits: CPU time, address space, file size, open files (via prlimit)
- network: denied by default; enforced with a private network namespace
  when available, otherwise explicitly degraded (observable in the result)
- privilege drop to an unprivileged uid/gid (via setpriv) when running as
  root; when the conditions are not met the executor reports DEGRADED
  instead of silently running with full privileges

Resource enforcement: wall-clock timeout, CPU time (rlimit), memory (process
group RSS polling), output volume (reader threads). Any breach kills the
whole process group; no processes are left behind.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import threading
import time

from .models import FailureKind, IsolationMode, JobResult, JobSpec, JobStatus

_SAFE_ENV = {
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "LANG": "C.UTF-8",
}
_RESERVED_ENV_KEYS = frozenset(_SAFE_ENV) | {
    "HOME",
    "TMPDIR",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
}
_POLL_INTERVAL_S = 0.05
# RLIMIT_AS is only a backstop: the real memory limit is enforced by RSS
# polling, so the address-space cap gets headroom for interpreter startup.
_RLIMIT_AS_HEADROOM = 512 * 1024 * 1024


def _have(binary: str) -> bool:
    return shutil.which(binary) is not None


def _unshare_net_available() -> bool:
    if not _have("unshare"):
        return False
    try:
        return (
            subprocess.run(
                ["unshare", "--net", "true"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            ).returncode
            == 0
        )
    except (OSError, subprocess.SubprocessError):
        return False


def _group_rss_bytes(pgid: int) -> int:
    """Sum VmRSS of every process in the group (best effort)."""
    total = 0
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", "rb") as fh:
                stat = fh.read()
            # pgrp is field 5; comm may contain spaces/parens, split after last ')'
            rest = stat[stat.rindex(b")") + 2 :].split()
            if int(rest[2]) != pgid:
                continue
            with open(f"/proc/{name}/status", "rb") as fh:
                for line in fh:
                    if line.startswith(b"VmRSS:"):
                        total += int(line.split()[1]) * 1024
                        break
        except (OSError, ValueError, IndexError):
            continue
    return total


class Executor:
    def __init__(
        self,
        work_root: str,
        drop_uid: int | None,
        drop_gid: int | None,
    ) -> None:
        self._work_root = os.path.abspath(work_root)
        os.makedirs(self._work_root, exist_ok=True)
        self._can_drop = (
            os.geteuid() == 0
            and drop_uid is not None
            and drop_gid is not None
            and _have("setpriv")
        )
        self._drop_uid = drop_uid if self._can_drop else None
        self._drop_gid = drop_gid if self._can_drop else None
        self._netns_available = _unshare_net_available()
        self._prlimit_available = _have("prlimit")

    def isolation_mode(self) -> IsolationMode:
        return IsolationMode.FULL if self._can_drop else IsolationMode.DEGRADED

    def capability_notes(self) -> list[str]:
        notes: list[str] = []
        if not self._can_drop:
            notes.append(
                "privilege drop unavailable (not root, no drop uid/gid, or"
                " setpriv missing); jobs run as the service user"
            )
        if not self._netns_available:
            notes.append(
                "network namespace isolation unavailable; network access"
                " cannot be enforced off"
            )
        if not self._prlimit_available:
            notes.append("prlimit missing; cpu/memory/file rlimits not enforced")
        return notes

    def _wrap_command(
        self, spec: JobSpec, network_granted: bool, notes: list[str]
    ) -> list[str]:
        limits = spec.limits
        cmd = list(spec.command)
        if self._can_drop:
            cmd = [
                "setpriv",
                f"--reuid={self._drop_uid}",
                f"--regid={self._drop_gid}",
                "--clear-groups",
                "--",
            ] + cmd
        else:
            notes.append("privilege drop unavailable: running as service user")
        if self._prlimit_available:
            cpu = max(1, int(limits.cpu_time_s))
            as_cap = limits.memory_bytes + _RLIMIT_AS_HEADROOM
            cmd = [
                "prlimit",
                f"--cpu={cpu}:{cpu + 1}",
                f"--as={as_cap}:{as_cap}",
                f"--fsize={limits.max_output_bytes}:{limits.max_output_bytes}",
                "--nofile=256:256",
                "--",
            ] + cmd
        if not network_granted:
            if self._netns_available:
                cmd = ["unshare", "--net", "--"] + cmd
            else:
                notes.append("network isolation unavailable: running without netns")
        return cmd

    def run(
        self,
        job_id: str,
        spec: JobSpec,
        network_granted: bool,
        cancel_event: threading.Event,
    ) -> JobResult:
        """Run the job synchronously; never raises."""
        notes: list[str] = []
        workdir = os.path.join(self._work_root, job_id)
        proc: subprocess.Popen[bytes] | None = None
        try:
            os.makedirs(os.path.join(workdir, "tmp"), exist_ok=True)
            if self._can_drop:
                os.chown(workdir, self._drop_uid, self._drop_gid)  # type: ignore[arg-type]
                os.chown(
                    os.path.join(workdir, "tmp"),
                    self._drop_uid,  # type: ignore[arg-type]
                    self._drop_gid,  # type: ignore[arg-type]
                )

            env = dict(_SAFE_ENV)
            env["HOME"] = workdir
            env["TMPDIR"] = os.path.join(workdir, "tmp")
            for key, value in spec.env.items():
                if key not in _RESERVED_ENV_KEYS:
                    env[key] = value

            # Pre-flight: the wrapper chain hides spawn failures of the inner
            # command, so resolve the executable up front.
            prog = spec.command[0] if spec.command else ""
            resolved = (
                prog
                if os.sep in prog
                else shutil.which(prog, path=env["PATH"])
            )
            if not prog or resolved is None or not os.access(resolved, os.X_OK):
                return JobResult(
                    job_id=job_id,
                    status=JobStatus.FAILED,
                    failure_kind=FailureKind.SPAWN_ERROR,
                    isolation=self.isolation_mode(),
                    network_granted=network_granted,
                    error=f"command not found or not executable: {prog!r}",
                    notes=notes,
                )

            cmd = self._wrap_command(spec, network_granted, notes)
            limits = spec.limits

            start = time.monotonic()
            deadline = start + limits.wall_timeout_s
            try:
                proc = subprocess.Popen(
                    cmd,
                    cwd=workdir,
                    env=env,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    process_group=0,
                )
            except OSError as exc:
                return JobResult(
                    job_id=job_id,
                    status=JobStatus.FAILED,
                    failure_kind=FailureKind.SPAWN_ERROR,
                    isolation=self.isolation_mode(),
                    network_granted=network_granted,
                    error=f"spawn failed: {exc}",
                    notes=notes,
                )

            kill_reason: dict[str, FailureKind | str] = {}
            kill_lock = threading.Lock()

            def _kill(reason: FailureKind | str) -> None:
                with kill_lock:
                    if kill_reason:
                        return
                    kill_reason["why"] = reason
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    try:
                        proc.kill()
                    except OSError:
                        pass

            out_cap = limits.max_output_bytes
            sizes = {"n": 0}
            sizes_lock = threading.Lock()
            stdout_chunks: list[bytes] = []
            stderr_chunks: list[bytes] = []

            def _reader(stream, sink: list[bytes]) -> None:
                try:
                    while True:
                        chunk = stream.read(65536)
                        if not chunk:
                            return
                        sink.append(chunk)
                        with sizes_lock:
                            sizes["n"] += len(chunk)
                            over = sizes["n"] > out_cap
                        if over:
                            _kill(FailureKind.OUTPUT_LIMIT)
                            return
                except (OSError, ValueError):
                    return

            assert proc.stdout is not None and proc.stderr is not None
            readers = [
                threading.Thread(
                    target=_reader, args=(proc.stdout, stdout_chunks), daemon=True
                ),
                threading.Thread(
                    target=_reader, args=(proc.stderr, stderr_chunks), daemon=True
                ),
            ]
            for t in readers:
                t.start()

            def _watcher() -> None:
                while proc.poll() is None:
                    if cancel_event.is_set():
                        _kill("cancelled")
                        return
                    if time.monotonic() > deadline:
                        _kill(FailureKind.WALL_TIMEOUT)
                        return
                    try:
                        if _group_rss_bytes(proc.pid) > limits.memory_bytes:
                            _kill(FailureKind.MEMORY_LIMIT)
                            return
                    except OSError:
                        pass
                    time.sleep(_POLL_INTERVAL_S)

            watcher = threading.Thread(target=_watcher, daemon=True)
            watcher.start()
            proc.wait()
            for t in readers:
                t.join(timeout=5)

            stdout = b"".join(stdout_chunks)[:out_cap].decode("utf-8", "replace")
            stderr = b"".join(stderr_chunks)[:out_cap].decode("utf-8", "replace")
            why = kill_reason.get("why")
            rc = proc.returncode

            base = dict(
                job_id=job_id,
                isolation=self.isolation_mode(),
                network_granted=network_granted,
                stdout=stdout,
                stderr=stderr,
                notes=notes,
            )
            if why == "cancelled":
                return JobResult(status=JobStatus.CANCELLED, **base)
            if isinstance(why, FailureKind):
                return JobResult(status=JobStatus.FAILED, failure_kind=why, **base)
            if rc == 0:
                return JobResult(status=JobStatus.SUCCEEDED, exit_code=0, **base)
            if rc is not None and rc < 0:
                sig = -rc
                kind = (
                    FailureKind.CPU_LIMIT
                    if sig == signal.SIGXCPU
                    else FailureKind.SIGNALLED
                )
                return JobResult(
                    status=JobStatus.FAILED, failure_kind=kind, signal=sig, **base
                )
            return JobResult(
                status=JobStatus.FAILED,
                failure_kind=FailureKind.NON_ZERO_EXIT,
                exit_code=rc,
                **base,
            )
        except Exception as exc:  # never let one job break the scheduler
            if proc is not None and proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except OSError:
                    pass
            return JobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                failure_kind=FailureKind.INTERNAL_ERROR,
                isolation=self.isolation_mode(),
                error=f"executor internal error: {exc!r}",
                notes=notes,
            )
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
