import asyncpg
from fastapi import APIRouter, Depends

from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.security import security
from functions.utils.utils import default_response
from schemas.pipeline import PipelineStartRequest
from services.pipeline import pipeline_service

router = APIRouter()


@router.post("/run")
async def start_pipeline(
    data: PipelineStartRequest,
    user: dict = Depends(security.validate_token_wrapper),
    channel=Depends(rabbitmq.get_channel),
):
    return await default_response(
        pipeline_service.start_pipeline_run,
        [channel, user["userId"], data],
    )


@router.get("/status")
async def get_pipeline_status(
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        pipeline_service.get_pipeline_status,
        [conn, user["userId"]],
    )
