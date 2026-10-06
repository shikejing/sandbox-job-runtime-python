"""Backward-compatible development entry point.

Prefer ``uvicorn sandbox.app:app --factory``; this module keeps the original
``python main.py`` workflow working.
"""

from __future__ import annotations


def _build():
    import uvicorn

    from sandbox.app import build_app

    uvicorn.run(build_app(), host="127.0.0.1", port=8000)


if __name__ == "__main__":
    _build()
