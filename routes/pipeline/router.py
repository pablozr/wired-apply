import asyncpg
from fastapi import APIRouter, Depends

from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from core.security import security
from functions.utils.utils import default_response
from schemas.pipeline import GlobalIngestionStartRequest, PipelineStartRequest
from services.pipeline import (
    global_catalog_cleanup_run_service,
    global_catalog_cleanup_status_service,
    global_ingestion_service,
    pipeline_service,
)

router = APIRouter()


@router.post("/run")
async def start_pipeline(
    data: PipelineStartRequest,
    user: dict = Depends(security.validate_token_wrapper),
    channel=Depends(rabbitmq.get_channel),
    redis_client=Depends(redis_cache.get_redis),
):
    return await default_response(
        pipeline_service.start_pipeline_run,
        [channel, user["userId"], data, redis_client],
    )


@router.get("/status")
async def get_pipeline_status(
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
    redis_client=Depends(redis_cache.get_redis),
):
    return await default_response(
        pipeline_service.get_pipeline_status,
        [conn, user["userId"], redis_client],
    )

@router.post("/global/run")
async def start_global_ingestion(
    data: GlobalIngestionStartRequest,
    user: dict = Depends(security.require_admin_rank()),
    channel=Depends(rabbitmq.get_channel),
    redis_client=Depends(redis_cache.get_redis),
):
    return await default_response(
        global_ingestion_service.start_global_ingestion_run,
        [channel, data, redis_client, user["userId"]],
    )


@router.get("/global/status")
async def get_global_ingestion_status(
    _user: dict = Depends(security.require_admin_rank()),
    redis_client=Depends(redis_cache.get_redis),
):
    return await default_response(
        global_ingestion_service.get_global_ingestion_status,
        [redis_client],
    )


@router.get("/global/catalog-cleanup/status")
async def get_global_catalog_cleanup_status(
    _user: dict = Depends(security.require_admin_rank()),
    redis_client=Depends(redis_cache.get_redis),
):
    return await default_response(
        global_catalog_cleanup_status_service.get_global_catalog_cleanup_status,
        [redis_client],
    )


@router.post("/global/catalog-cleanup/run")
async def run_global_catalog_cleanup(
    user: dict = Depends(security.require_admin_rank()),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
    redis_client=Depends(redis_cache.get_redis),
):
    return await default_response(
        global_catalog_cleanup_run_service.run_global_catalog_cleanup,
        [conn, redis_client, user["userId"], "manual"],
    )
