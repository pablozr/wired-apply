import asyncpg
from fastapi import APIRouter, Depends, Query

from core.postgresql.postgresql import postgresql
from core.security import security
from functions.utils.utils import default_response
from schemas.jobs import JobCreateRequest, JobUpdateRequest
from services.jobs import jobs_service
from services.ranking import ranking_service

router = APIRouter()


@router.get("/")
async def list_jobs(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        jobs_service.list_jobs,
        [conn, user["userId"], limit, offset],
    )


@router.post("/")
async def create_job(
    data: JobCreateRequest,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        jobs_service.create_job,
        [conn, user["userId"], data],
        is_creation=True,
    )


@router.get("/ranking/daily")
async def get_daily_ranking(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        ranking_service.get_daily_ranking,
        [conn, user["userId"], limit, offset],
    )


@router.get("/{job_id}/score")
async def get_job_score(
    job_id: int,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        ranking_service.get_job_score,
        [conn, user["userId"], job_id],
    )


@router.get("/{job_id}")
async def get_job(
    job_id: int,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(jobs_service.get_one_job, [conn, user["userId"], job_id])


@router.put("/{job_id}")
async def update_job(
    job_id: int,
    data: JobUpdateRequest,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        jobs_service.update_job,
        [conn, user["userId"], job_id, data],
    )


@router.delete("/{job_id}")
async def delete_job(
    job_id: int,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(jobs_service.delete_job, [conn, user["userId"], job_id])
