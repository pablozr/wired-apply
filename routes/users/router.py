import asyncpg
from fastapi import APIRouter, Depends

from core.postgresql.postgresql import postgresql
from core.security import security
from functions.utils.utils import default_response
from schemas.user import UserCreateRequest, UserUpdateRequest
from services.user import user_service

router = APIRouter()


@router.get("/me")
async def get_me(
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(user_service.get_one_user, [conn, user["userId"]])


@router.put("/me")
async def update_me(
    data: UserUpdateRequest,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(user_service.update_me, [conn, user["userId"], data])


@router.post("/")
async def create_user(
    data: UserCreateRequest,
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(user_service.create, [conn, data], is_creation=True)
