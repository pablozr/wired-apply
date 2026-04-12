from datetime import date

import asyncpg
from fastapi import APIRouter, Depends, Query

from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.security import security
from functions.utils.utils import default_response
from schemas.digest import DigestGenerateRequest
from services.digest import digest_service

router = APIRouter()


@router.post("/generate")
async def generate_daily_digest(
    data: DigestGenerateRequest,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
    channel=Depends(rabbitmq.get_channel),
):
    return await default_response(
        digest_service.generate_daily_digest,
        [conn, channel, user["userId"], data],
    )


@router.get("/daily")
async def get_daily_digest(
    digest_date: date | None = Query(default=None, alias="digestDate"),
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        digest_service.get_daily_digest,
        [conn, user["userId"], digest_date],
    )
