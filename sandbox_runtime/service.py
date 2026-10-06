"""FastAPI application wiring: submit / query / cancel + observability."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .config import Settings
from .executor import Executor
from .models import JobResult, JobSpec, ResourceLimits
from .scheduler import Scheduler
from .store import JobStore


class LimitsIn(BaseModel):
    wall_timeout_s: float = Field(default=30.0, gt=0)
    cpu_time_s: float = Field(default=20.0, gt=0)
    memory_bytes: int = Field(default=256 * 1024 * 1024, gt=0)
    max_output_bytes: int = Field(default=1024 * 1024, gt=0)


class SubmitIn(BaseModel):
    submitter: str = Field(min_length=1, max_length=128)
    command: list[str] = Field(min_length=1)
    env: dict[str, str] = Field(default_factory=dict)
    limits: LimitsIn = Field(default_factory=LimitsIn)
    allow_network: bool = False


class SubmitOut(BaseModel):
    job_id: str
    status: str


class ResultOut(BaseModel):
    job_id: str
    status: str
    exit_code: int | None = None
    signal: int | None = None
    failure_kind: str | None = None
    stdout: str = ""
    stderr: str = ""
    isolation: str | None = None
    network_granted: bool = False
    error: str | None = None
    notes: list[str] = Field(default_factory=list)

    @classmethod
    def from_result(cls, r: JobResult) -> "ResultOut":
        return cls(
            job_id=r.job_id,
            status=r.status.value,
            exit_code=r.exit_code,
            signal=r.signal,
            failure_kind=r.failure_kind.value if r.failure_kind else None,
            stdout=r.stdout,
            stderr=r.stderr,
            isolation=r.isolation.value if r.isolation else None,
            network_granted=r.network_granted,
            error=r.error,
            notes=r.notes,
        )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    os.makedirs(settings.data_dir, exist_ok=True)
    store = JobStore(os.path.join(settings.data_dir, "jobs.db"))
    store.open()
    executor = Executor(
        os.path.join(settings.data_dir, "work"),
        drop_uid=settings.drop_uid if settings.drop_uid is not None else 65534,
        drop_gid=settings.drop_gid if settings.drop_gid is not None else 65534,
    )
    scheduler = Scheduler(settings, store, executor)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        scheduler.recover()
        scheduler.start()
        yield
        scheduler.shutdown()
        store.close()

    app = FastAPI(title="sandbox-job-runtime", lifespan=lifespan)
    app.state.settings = settings
    app.state.scheduler = scheduler
    app.state.executor = executor

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/capabilities")
    def capabilities() -> dict:
        return {
            "isolation": executor.isolation_mode().value,
            "notes": executor.capability_notes(),
            "network_policy_allow": settings.network_policy_allow,
            "global_concurrency": settings.global_concurrency,
            "per_submitter_concurrency": settings.per_submitter_concurrency,
        }

    @app.post("/jobs", response_model=SubmitOut, status_code=201)
    def submit(body: SubmitIn) -> SubmitOut:
        spec = JobSpec(
            submitter=body.submitter,
            command=body.command,
            env=body.env,
            limits=ResourceLimits(**body.limits.model_dump()),
            allow_network=body.allow_network,
        )
        job_id = scheduler.submit(spec)
        return SubmitOut(job_id=job_id, status="queued")

    @app.get("/jobs/{job_id}", response_model=ResultOut)
    def get_job(job_id: str) -> ResultOut:
        result = scheduler.get(job_id)
        if result is None:
            raise HTTPException(status_code=404, detail="job not found")
        return ResultOut.from_result(result)

    @app.post("/jobs/{job_id}/cancel", response_model=ResultOut)
    def cancel_job(job_id: str) -> ResultOut:
        if scheduler.get(job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        if not scheduler.cancel(job_id):
            raise HTTPException(status_code=409, detail="job already terminal")
        result = scheduler.get(job_id)
        assert result is not None
        return ResultOut.from_result(result)

    return app
