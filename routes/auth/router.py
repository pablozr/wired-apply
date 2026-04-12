import asyncpg
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from core.config.config import (
    AUTH_COOKIE_MAX_AGE,
    COOKIE_AUTH,
    COOKIE_AUTH_RESET,
    RESET_COOKIE_MAX_AGE,
)
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from core.security import security
from core.security.rate_limit import (
    FORGET_PASSWORD_RATE_LIMIT_DEPS,
    LOGIN_RATE_LIMIT_DEPS,
    VALIDATE_CODE_RATE_LIMIT_DEPS,
)
from schemas.auth import (
    ForgetPasswordRequestModel,
    LoginRequestModel,
    UpdatePasswordRequest,
    ValidateCodeRequest,
)
from services.auth import auth_service

router = APIRouter()


@router.post("/login", dependencies=LOGIN_RATE_LIMIT_DEPS)
async def login(
    data: LoginRequestModel, conn: asyncpg.Connection = Depends(postgresql.get_db)
):
    response = await auth_service.login(conn, data)

    if not response["status"]:
        return JSONResponse(status_code=400, content={"detail": response["message"]})

    token = response["data"].pop("access_token")
    resp = JSONResponse(
        status_code=200,
        content={"message": response["message"], "data": response["data"]},
    )
    resp.set_cookie(
        key=COOKIE_AUTH,
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
        max_age=AUTH_COOKIE_MAX_AGE,
    )

    return resp


@router.post("/logout")
async def logout():
    resp = JSONResponse(
        status_code=200,
        content={"message": "Logged out", "data": {}},
    )
    resp.delete_cookie(key=COOKIE_AUTH, path="/", samesite="lax")
    resp.delete_cookie(key=COOKIE_AUTH_RESET, path="/", samesite="lax")

    return resp


@router.post("/forget-password", dependencies=FORGET_PASSWORD_RATE_LIMIT_DEPS)
async def forget_password(
    data: ForgetPasswordRequestModel,
    conn: asyncpg.Connection = Depends(postgresql.get_db),
    redis_client=Depends(redis_cache.get_redis),
    channel=Depends(rabbitmq.get_channel),
):
    response = await auth_service.forget_password(conn, redis_client, channel, data)

    if not response["status"]:
        return JSONResponse(status_code=400, content={"detail": response["message"]})

    token = response["data"].pop("access_token")
    resp = JSONResponse(
        status_code=200,
        content={"message": response["message"], "data": response["data"]},
    )
    resp.set_cookie(
        key=COOKIE_AUTH_RESET,
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
        max_age=RESET_COOKIE_MAX_AGE,
    )

    return resp


@router.post("/validate-code", dependencies=VALIDATE_CODE_RATE_LIMIT_DEPS)
async def validate_code(
    data: ValidateCodeRequest,
    user: dict = Depends(security.validate_token_to_validate_code),
    redis_client=Depends(redis_cache.get_redis),
):
    response = await auth_service.validate_reset_code(redis_client, user, data)

    if not response["status"]:
        return JSONResponse(status_code=400, content={"detail": response["message"]})

    token = response["data"].pop("access_token")
    resp = JSONResponse(
        status_code=200,
        content={"message": response["message"], "data": response["data"]},
    )
    resp.set_cookie(
        key=COOKIE_AUTH_RESET,
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
        max_age=RESET_COOKIE_MAX_AGE,
    )

    return resp


@router.post("/update-password")
async def update_password(
    data: UpdatePasswordRequest,
    user: dict = Depends(security.validate_token_to_update_password),
    conn: asyncpg.Connection = Depends(postgresql.get_db),
):
    response = await auth_service.update_password_after_reset(conn, user, data)

    if not response["status"]:
        return JSONResponse(status_code=400, content={"detail": response["message"]})

    resp = JSONResponse(
        status_code=200,
        content={"message": response["message"], "data": response["data"]},
    )
    resp.delete_cookie(key=COOKIE_AUTH_RESET, path="/", samesite="lax")

    return resp
