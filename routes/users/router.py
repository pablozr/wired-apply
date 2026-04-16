import asyncpg
from fastapi import APIRouter, Depends, File, UploadFile

from core.postgresql.postgresql import postgresql
from core.security import security
from functions.utils.utils import default_response
from schemas.profile import UserProfileUpsertRequest
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


@router.get("/me/profile")
async def get_my_profile(
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(user_service.get_my_profile, [conn, user["userId"]])


@router.put("/me/profile")
async def upsert_my_profile(
    data: UserProfileUpsertRequest,
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        user_service.upsert_my_profile,
        [conn, user["userId"], data],
    )


@router.post("/me/resume")
async def upload_my_resume(
    file: UploadFile = File(...),
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(
        user_service.upload_my_resume,
        [conn, user["userId"], file],
    )


@router.get("/me/resume")
async def get_my_resume(
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(user_service.get_my_resume, [conn, user["userId"]])


@router.delete("/me/resume")
async def delete_my_resume(
    user: dict = Depends(security.validate_token_wrapper),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(user_service.delete_my_resume, [conn, user["userId"]])


@router.post("/")
async def create_user(
    data: UserCreateRequest,
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    return await default_response(user_service.create, [conn, data], is_creation=True)
