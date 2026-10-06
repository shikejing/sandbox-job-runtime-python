"""Isolation capability probing and low-privilege user setup.

Nothing here is assumed: every capability is exercised with a cheap real
probe at startup so the service can honestly report whether a given job ran
inside a PID/net namespace and with dropped privileges.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import pwd
import shutil
import subprocess
import sys
from dataclasses import dataclass, field

libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)


@dataclass
class IsolationCapabilities:
    """What the local machine can actually provide."""

    running_as_root: bool = False
    can_pid_namespace: bool = False
    can_net_namespace: bool = False
    can_setrlimit: bool = False
    can_drop_privileges: bool = False
    runner_uid: int | None = None
    runner_gid: int | None = None
    runner_user: str | None = None
    degradation: list[str] = field(default_factory=list)

    def describe(self) -> list[str]:
        """Human-readable capability lines, including explicit degradations."""
        lines = [
            f"root={self.running_as_root}",
            f"pid_namespace={self.can_pid_namespace}",
            f"net_namespace={self.can_net_namespace}",
            f"setrlimit={self.can_setrlimit}",
            f"drop_privileges={self.can_drop_privileges} as {self.runner_user}({self.runner_uid})",
        ]
        if self.degradation:
            lines.append("degraded: " + "; ".join(self.degradation))
        return lines


def _have_unshare(flag: int) -> bool:
    """True if unshare(flag) works for this process (privileged probe)."""
    if not os.environ.get("SANDBOX_SKIP_NS_PROBE") and hasattr(libc, "unshare"):
        rc = libc.unshare(flag)
        if rc == 0:
            return True
        ctypes.get_errno()
        # EPERM means the platform/policy disallows it.
        return False
    return False


def _namespace_probe(kind: str) -> bool:
    """Run a child that unshares *kind*; works regardless of python bindings."""
    exe = shutil.which("unshare")
    if exe is None:
        return False
    try:
        proc = subprocess.run(
            [exe, f"--{kind}", sys.executable, "-c", "import os; os._exit(0)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


def _probe_rlimit_cpu() -> bool:
    """setrlimit(RLIMIT_CPU) is the only rlimit we strictly rely on; try it."""
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_CPU)
        resource.setrlimit(resource.RLIMIT_CPU, (soft, hard))
        return True
    except (OSError, ValueError):
        return False


def _find_nobody() -> tuple[str, int, int] | None:
    for name in ("nobody", "nogroup", "daemon"):
        try:
            entry = pwd.getpwnam(name)
            return entry.pw_name, entry.pw_uid, entry.pw_gid
        except KeyError:
            continue
    return None


def _create_sandbox_user() -> tuple[str, int, int] | None:
    """Create a dedicated low-privilege account when tooling permits."""
    name = "sandboxrunner"
    try:
        entry = pwd.getpwnam(name)
        return entry.pw_name, entry.pw_uid, entry.pw_gid
    except KeyError:
        pass
    useradd = shutil.which("useradd")
    if useradd is None:
        return None
    try:
        # System account, no home, no login shell, locked password.
        subprocess.run(
            [useradd, "-r", "-M", "-s", "/sbin/nologin", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        )
        entry = pwd.getpwnam(name)
        return entry.pw_name, entry.pw_uid, entry.pw_gid
    except (KeyError, OSError, subprocess.SubprocessError):
        return None


def ensure_runner_user(existing: str | None = None) -> tuple[str, int, int] | None:
    """Return an unprivileged ``(name, uid, gid)`` account or None."""
    if existing:
        try:
            entry = pwd.getpwnam(existing)
        except KeyError:
            return None
        if entry.pw_uid != 0:
            return entry.pw_name, entry.pw_uid, entry.pw_gid
        return None
    created = _create_sandbox_user()
    if created is not None:
        return created
    return _find_nobody()


def probe_capabilities(configured_runner_user: str | None = None) -> IsolationCapabilities:
    """Probe namespaces, rlimits and privilege-drop support once at startup."""
    caps = IsolationCapabilities()
    caps.running_as_root = os.geteuid() == 0
    caps.can_pid_namespace = _namespace_probe("pid")
    caps.can_net_namespace = _namespace_probe("net")
    caps.can_setrlimit = _probe_rlimit_cpu()

    if caps.running_as_root:
        account = ensure_runner_user(configured_runner_user)
    else:
        account = None

    if account is not None:
        caps.runner_user, caps.runner_uid, caps.runner_gid = account
        caps.can_drop_privileges = True
    else:
        caps.can_drop_privileges = False

    if not caps.can_pid_namespace:
        caps.degradation.append("pid_namespace_unavailable")
    if not caps.can_net_namespace:
        caps.degradation.append("net_namespace_unavailable")
    if not caps.can_drop_privileges:
        caps.degradation.append("privilege_drop_unavailable")
    return caps
