"""ASGI entry point and ``python -m`` support.

Run with either::

    uvicorn sandbox.app:app --factory
    uvicorn sandbox.app:build_app --factory
    python main.py
"""

from __future__ import annotations

import logging
import os

from fastapi import FastAPI

from .api import create_app
from .config import load_config
from .service import SandboxService

_service: SandboxService | None = None


def get_service() -> SandboxService:
    if _service is None:
        raise RuntimeError("service not initialised")
    return _service


def build_app() -> FastAPI:
    """Construct the application and start background components."""
    global _service
    logging.basicConfig(
        level=os.environ.get("SANDBOX_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _service = SandboxService(load_config()).start()
    application = create_app(_service)

    @application.on_event("shutdown")
    def _shutdown() -> None:
        if _service is not None:
            _service.shutdown()

    return application


# Importable ASGI application for ``sandbox.app:app`` without --factory.
app = build_app()
