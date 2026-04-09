from datetime import datetime, timedelta, timezone

import asyncpg
import jwt
from fastapi import Depends, HTTPException, Request
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from core.config.config import settings
from core.config.config import COOKIE_AUTH, COOKIE_AUTH_RESET, ROLE_RANK_BY_NAME
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from services.user import user_service


def decode_access_token(token: str) -> dict:
    return jwt.decode(
        token,
        settings.SECRET_KEY,
        algorithms=[settings.ALGORITHM],
    )


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    payload = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    payload.update({"exp": expire})

    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def verify_google_token(token: str) -> dict | None:
    try:
        return id_token.verify_oauth2_token(
            token, google_requests.Request(), settings.GOOGLE_CLIENT_ID
        )
    except Exception:
        return None


async def verify_token(
    token: str,
    conn: asyncpg.Connection,
    check_can_update: bool = False,
    expected_type: str = "auth",
) -> dict | bool | None:
    try:
        if token.startswith("Bearer "):
            token = token[7:]

        payload = decode_access_token(token)

        if payload.get("type") != expected_type:
            raise jwt.InvalidTokenError("Token type mismatch")

        if expected_type == "reset" and not check_can_update:
            if payload.get("canUpdate") is not False:
                raise jwt.InvalidTokenError("Invalid reset token stage")

        if not payload.get("userId"):
            raise jwt.InvalidTokenError("Invalid token payload")

        response = await user_service.get_one_user(conn, payload["userId"])

        if response["status"] is None or not response["status"]:
            raise jwt.InvalidSignatureError("User not found")

        if check_can_update:
            if payload.get("canUpdate"):
                return dict(response["data"]["user"])
            raise jwt.InvalidTokenError("User does not have update permissions")

        return dict(response["data"]["user"])

    except jwt.ExpiredSignatureError:
        logger.error("Token has expired")
        return None
    except jwt.InvalidTokenError:
        logger.error("Invalid token")
        return False


async def validate_token(
    request: Request,
    conn: asyncpg.Connection,
    check_can_update: bool = False,
    reset_cookie: bool = False,
    expected_type: str = "auth",
) -> dict:
    try:
        cookie_key = COOKIE_AUTH if not reset_cookie else COOKIE_AUTH_RESET
        token = request.cookies.get(cookie_key)

        if not token:
            raise HTTPException(status_code=401, detail="Not authenticated")

        user = await verify_token(
            token,
            conn=conn,
            check_can_update=check_can_update,
            expected_type=expected_type,
        )

        if user is None:
            raise HTTPException(status_code=401, detail="Token has expired")

        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")

        request.state.token = token

        return user

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=401, detail="Invalid token")


async def validate_token_to_update_password(
    request: Request, conn: asyncpg.Connection = Depends(postgresql.get_db)
) -> dict:
    return await validate_token(
        request,
        conn,
        check_can_update=True,
        reset_cookie=True,
        expected_type="reset",
    )


async def validate_token_to_validate_code(
    request: Request, conn: asyncpg.Connection = Depends(postgresql.get_db)
) -> dict:
    return await validate_token(
        request,
        conn,
        check_can_update=False,
        reset_cookie=True,
        expected_type="reset",
    )


async def validate_token_wrapper(
    request: Request, conn: asyncpg.Connection = Depends(postgresql.get_db)
) -> dict:
    return await validate_token(request, conn)


def require_minimum_rank(minimum_rank: int):
    async def dependency(user: dict = Depends(validate_token_wrapper)) -> dict:
        rank = ROLE_RANK_BY_NAME.get(user.get("role", "").upper(), 0)

        if rank < minimum_rank:
            raise HTTPException(status_code=403, detail="Insufficient permissions")

        return user

    return dependency


def require_admin_rank():
    return require_minimum_rank(2)
