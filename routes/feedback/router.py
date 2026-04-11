import asyncpg
from fastapi import APIRouter, Depends, Query

from core.postgresql.postgresql import postgresql
from core.security import security
from functions.utils.utils import default_response
from schemas.feedback import FeedbackCreateRequest, FeedbackUpdateRequest
from services.feedback import feedback_service

router = APIRouter()


@router.get("/")
async def list_feedback(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    rating: int | None = Query(default=None, ge=1, le=5),
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        feedback_service.list_feedback,
        [conn, user["userId"], limit, offset, rating],
    )


@router.post("/")
async def create_feedback(
    data: FeedbackCreateRequest,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        feedback_service.create_feedback,
        [conn, user["userId"], data],
        is_creation=True,
    )


@router.get("/{feedback_id}")
async def get_feedback(
    feedback_id: int,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        feedback_service.get_one_feedback,
        [conn, user["userId"], feedback_id],
    )


@router.put("/{feedback_id}")
async def update_feedback(
    feedback_id: int,
    data: FeedbackUpdateRequest,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        feedback_service.update_feedback,
        [conn, user["userId"], feedback_id, data],
    )


@router.delete("/{feedback_id}")
async def delete_feedback(
    feedback_id: int,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        feedback_service.delete_feedback,
        [conn, user["userId"], feedback_id],
    )
