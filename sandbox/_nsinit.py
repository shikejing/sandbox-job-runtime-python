"""Minimal PID-1 reaper used inside an unshared PID namespace.

The payload must not itself be PID 1: namespace init ignores the default
disposition of fatal signals (SIGSEGV etc. would be swallowed).  This helper

* forks once and execs the payload chain in the child,
* forwards common termination signals to that child,
* reaps every child so daemons do not accumulate zombies,
* records the child's true wait status (exit vs. fatal signal) into
  ``$SANDBOX_NS_STATUS`` so the supervisor can report it honestly,
* exits with the child's code, or 128+signal and a status marker.

If this init itself is SIGKILLed the kernel destroys the whole namespace,
which guarantees no process survives teardown.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import sys

_FORWARDED = (
    signal.SIGHUP,
    signal.SIGINT,
    signal.SIGQUIT,
    signal.SIGTERM,
    signal.SIGUSR1,
    signal.SIGUSR2,
)


def _write_status(payload: dict) -> None:
    path = os.environ.get("SANDBOX_NS_STATUS")
    if not path:
        return
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        os.replace(tmp, path)
    except OSError:
        pass


def main(argv: list[str]) -> int:
    if not argv:
        return 127

    child = os.fork()
    if child == 0:
        # Restore default handlers inherited from the parent before execve.
        for sig in _FORWARDED:
            with contextlib.suppress(OSError, ValueError):
                signal.signal(sig, signal.SIG_DFL)
        try:
            os.execvp(argv[0], argv)
        except OSError:
            os._exit(127)

    def _forward(signum, _frame):  # pragma: no cover - signal path
        with contextlib.suppress(ProcessLookupError):
            os.kill(child, signum)

    for sig in _FORWARDED:
        with contextlib.suppress(OSError, ValueError):
            signal.signal(sig, _forward)

    target_status = 0
    # Reap until our direct payload child is observed.
    while True:
        try:
            wpid, status = os.waitpid(-1, 0)
        except ChildProcessError:
            break
        if wpid == child:
            target_status = status
            break
    # The payload is gone; reap every grandchild reparented to this init so
    # none is left as a zombie while the namespace tears down.
    while True:
        try:
            os.waitpid(-1, 0)
        except ChildProcessError:
            break

    if os.WIFEXITED(target_status):
        code = os.WEXITSTATUS(target_status)
        _write_status({"kind": "exit", "code": code})
        return code
    if os.WIFSIGNALED(target_status):
        sig = os.WTERMSIG(target_status)
        _write_status({"kind": "signal", "signal": sig})
        return 128 + sig
    _write_status({"kind": "unknown", "raw": target_status})
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
