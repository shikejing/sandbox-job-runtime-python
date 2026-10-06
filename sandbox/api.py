"""HTTP API: submit, query and cancel jobs."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .config import Config
from .model import (
    FailureReason,
    Job,
    ResourceLimits,
    JobState,
)
from .service import SandboxService


class LimitsIn(BaseModel):
    wall_time_s: int | None = Field(default=None, ge=1)
    cpu_time_s: int | None = Field(default=None, ge=1)
    memory_mb: int | None = Field(default=None, ge=1)
    output_bytes: int | None = Field(default=None, ge=1)


class SubmitJobIn(BaseModel):
    submitter: str = Field(min_length=1, max_length=128)
    argv: list[str] = Field(min_length=1)
    request_network: bool = False
    env: dict[str, str] = Field(default_factory=dict)
    limits: LimitsIn | None = None


def _clamp_limits(cfg: Config, data: LimitsIn | None) -> ResourceLimits:
    data = data or LimitsIn()
    return ResourceLimits(
        wall_time_s=min(
            data.wall_time_s or cfg.default_wall_time_limit_s,
            cfg.max_wall_time_limit_s,
        ),
        cpu_time_s=min(
            data.cpu_time_s or cfg.default_cpu_time_limit_s,
            cfg.max_cpu_time_limit_s,
        ),
        memory_mb=min(
            data.memory_mb or cfg.default_memory_limit_mb,
            cfg.max_memory_limit_mb,
        ),
        output_bytes=min(
            data.output_bytes or cfg.default_output_limit_bytes,
            cfg.max_output_limit_bytes,
        ),
    )


def _job_json(job: Job) -> dict[str, Any]:
    data = job.to_dict()
    data["state"] = job.state.value
    if job.result is not None:
        data["result"]["failure_reason"] = job.result.failure_reason
    return data


def create_app(service: SandboxService) -> FastAPI:
    application = FastAPI(
        title="sandbox-job-runtime",
        version="0.1.0",
        description=(
            "Submits untrusted commands to a locally isolated runtime. "
            "Network is closed by default; jobs run unprivileged under "
            "wall/CPU/memory/output limits."
        ),
    )

    store = service.store
    cfg = service.config
    caps = service.caps
    assert store is not None and caps is not None

    @application.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "recovered_finished": service.recovered_finished,
            "recovered_interrupted": service.recovered_interrupted,
            "isolation_degraded": caps.degradation,
        }

    @application.get("/capabilities")
    def capabilities() -> dict[str, Any]:
        """Honest, observable report of the local isolation strength."""
        return {
            "running_as_root": caps.running_as_root,
            "pid_namespace": caps.can_pid_namespace,
            "net_namespace": caps.can_net_namespace,
            "setrlimit": caps.can_setrlimit,
            "privilege_drop": {
                "available": caps.can_drop_privileges,
                "user": caps.runner_user,
                "uid": caps.runner_uid,
                "gid": caps.runner_gid,
            },
            "network_default": "closed",
            "allow_network": cfg.allow_network,
            "allow_network_on_degraded_isolation":
                cfg.allow_network_on_degraded_isolation,
            "degradation": caps.degradation,
            "max_global_concurrency": cfg.max_global_concurrency,
            "max_per_submitter_concurrency":
                cfg.max_per_submitter_concurrency,
        }

    @application.post("/jobs", status_code=201)
    def submit_job(body: SubmitJobIn) -> dict[str, Any]:
        # Network policy gate, applied up-front as well as inside the runner.
        # Default deny even with a usable net namespace; the operator must
        # explicitly enable the feature, and degraded hosts are refused too.
        if body.request_network:
            if not cfg.allow_network:
                raise HTTPException(
                    status_code=403,
                    detail={
                        "error": FailureReason.NETWORK_POLICY_DENIED.value,
                        "message": (
                            "network access is disabled by service policy "
                            "(set SANDBOX_ALLOW_NETWORK=true to permit "
                            "explicitly requested, namespace-isolated jobs)"
                        ),
                    },
                )
            if not caps.can_net_namespace:
                if not cfg.allow_network_on_degraded_isolation:
                    raise HTTPException(
                        status_code=403,
                        detail={
                            "error": FailureReason.NETWORK_POLICY_DENIED.value,
                            "message": (
                                "network requested but the host cannot provide a "
                                "network namespace; refusing to run untrusted code "
                                "with open network access under degraded isolation"
                            ),
                        },
                    )

        # argv entries must be single non-empty program args.
        if not body.argv or not all(isinstance(a, str) for a in body.argv):
            raise HTTPException(status_code=400, detail="argv must be non-empty strings")
        if not body.argv[0]:
            raise HTTPException(status_code=400, detail="argv[0] must be non-empty")
        if len(body.env) > 64:
            raise HTTPException(status_code=400, detail="too many env entries")
        for key in body.env:
            if not key or "=" in key or "\x00" in key:
                raise HTTPException(status_code=400, detail=f"bad env key: {key!r}")

        limits = _clamp_limits(cfg, body.limits)
        job = Job.new(
            submitter=body.submitter,
            argv=body.argv,
            limits=limits,
            request_network=body.request_network,
            env=body.env,
        )
        try:
            store.create(job)
        except Exception as exc:  # JobExistsError practically cannot happen (uuid)
            raise HTTPException(status_code=409, detail=str(exc))
        return _job_json(job)

    @application.get("/jobs")
    def list_jobs(submitter: str | None = None) -> dict[str, Any]:
        jobs = store.list(submitter=submitter)
        return {"jobs": [_job_json(j) for j in jobs]}

    @application.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_json(job)

    @application.post("/jobs/{job_id}/cancel", status_code=202)
    def cancel_job(job_id: str) -> dict[str, Any]:
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.state in (JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED):
            return {
                "job_id": job_id,
                "cancel_requested": False,
                "state": job.state.value,
                "message": "job already terminal",
            }
        ok = store.request_cancel(job_id)
        current = store.get(job_id)
        # A still-queued job is finalized by the scheduler at its next dispatch
        # point (same lock that admits jobs), so cancel and start can never
        # both win.  A running job is terminated by its supervisor.
        return {
            "job_id": job_id,
            "cancel_requested": ok,
            "state": current.state.value if current else job.state.value,
        }

    return application
