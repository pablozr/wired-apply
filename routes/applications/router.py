import asyncpg
from fastapi import APIRouter, Depends, Query

from core.postgresql.postgresql import postgresql
from core.security import security
from functions.utils.utils import default_response
from schemas.applications import ApplicationCreateRequest, ApplicationUpdateRequest
from services.applications import application_service

router = APIRouter()


@router.get("/")
async def list_applications(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        application_service.list_applications,
        [conn, user["userId"], limit, offset, status],
    )


@router.post("/")
async def create_application(
    data: ApplicationCreateRequest,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        application_service.create_application,
        [conn, user["userId"], data],
        is_creation=True,
    )


@router.get("/{application_id}")
async def get_application(
    application_id: int,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        application_service.get_one_application,
        [conn, user["userId"], application_id],
    )


@router.put("/{application_id}")
async def update_application(
    application_id: int,
    data: ApplicationUpdateRequest,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        application_service.update_application,
        [conn, user["userId"], application_id, data],
    )


@router.delete("/{application_id}")
async def delete_application(
    application_id: int,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        application_service.delete_application,
        [conn, user["userId"], application_id],
    )
