from datetime import timedelta

import aio_pika
import asyncpg

from core.config.config import (
    AUTH_COOKIE_MAX_AGE,
    EMAIL_QUEUE,
    RESET_CODE_REDIS_TTL,
    RESET_COOKIE_MAX_AGE,
    settings,
)
from core.security.hashing import hash_password, verify_password
from core.security.jwt_payloads import auth_jwt_payload_from_row, reset_jwt_payload
from core.security.security import create_access_token
from functions.utils.utils import generate_temp_code
from schemas.auth import (
    ForgetPasswordRequestModel,
    LoginRequestModel,
    UpdatePasswordRequest,
    ValidateCodeRequest,
)
from schemas.user import user_from_row
from services.cache import cache_service
from services.common import internal_error
from services.messaging import messaging_service
from templates.email import RESET_PASSWORD_EMAIL_TEMPLATE


async def login(conn: asyncpg.Connection, data: LoginRequestModel) -> dict:
    try:
        row = await conn.fetchrow(
            """
            SELECT id, fullname, email, role, password, created_at
            FROM users WHERE email = $1
            """,
            data.email,
        )

        if not row or not verify_password(data.password, row["password"]):
            return {"status": False, "message": "Invalid email or password", "data": {}}

        token = create_access_token(
            auth_jwt_payload_from_row(row),
            expires_delta=timedelta(seconds=AUTH_COOKIE_MAX_AGE),
        )
        u = user_from_row(row)

        return {
            "status": True,
            "message": "Login successful",
            "data": {"user": u, "access_token": token},
        }
    except Exception as e:
        return internal_error(e)


async def forget_password(
    conn: asyncpg.Connection,
    redis_client,
    channel: aio_pika.abc.AbstractChannel,
    data: ForgetPasswordRequestModel,
) -> dict:
    try:
        row = await conn.fetchrow(
            "SELECT id, fullname, email, role FROM users WHERE email = $1", data.email
        )

        if not row:
            return {"status": False, "message": "User not found", "data": {}}

        code = generate_temp_code()
        cache_key = f"{row['id']}:{row['email']}"

        await cache_service.set_by_key(
            cache_key, RESET_CODE_REDIS_TTL, {"code": code}, redis_client
        )

        html = RESET_PASSWORD_EMAIL_TEMPLATE.replace("CODE_HERE", code)

        await messaging_service.publish(
            EMAIL_QUEUE,
            {
                "to": data.email,
                "from": settings.EMAIL_FROM,
                "subject": "Password reset code",
                "html": html,
                "message": "",
                "base64Attachment": "",
                "base64AttachmentName": "",
            },
            channel,
        )

        reset_payload = reset_jwt_payload(
            row["id"],
            row["email"],
            row["fullname"],
            row["role"],
            can_update=False,
        )
        token = create_access_token(
            reset_payload, expires_delta=timedelta(seconds=RESET_COOKIE_MAX_AGE)
        )

        return {
            "status": True,
            "message": "Verification code sent",
            "data": {"access_token": token},
        }
    except Exception as e:
        return internal_error(e)


async def validate_reset_code(
    redis_client, user: dict, data: ValidateCodeRequest
) -> dict:
    try:
        cache_key = f"{user['userId']}:{user['email']}"
        redis_data = await cache_service.get_by_key(cache_key, redis_client)

        if not redis_data or redis_data.get("code") != data.code:
            return {"status": False, "message": "Invalid or expired code", "data": {}}

        await cache_service.delete_by_key(cache_key, redis_client)

        reset_payload = reset_jwt_payload(
            user["userId"],
            user["email"],
            user["fullname"],
            user["role"],
            can_update=True,
        )
        token = create_access_token(
            reset_payload, expires_delta=timedelta(seconds=RESET_COOKIE_MAX_AGE)
        )

        return {
            "status": True,
            "message": "Code validated",
            "data": {"access_token": token},
        }
    except Exception as e:
        return internal_error(e)


async def update_password_after_reset(
    conn: asyncpg.Connection, user: dict, data: UpdatePasswordRequest
) -> dict:
    try:
        hashed = hash_password(data.password)

        row = await conn.fetchrow(
            """
            UPDATE users SET password = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING id, fullname, email, role, created_at
            """,
            hashed,
            user["userId"],
        )

        if not row:
            return {"status": False, "message": "User not found", "data": {}}

        return {
            "status": True,
            "message": "Password updated successfully",
            "data": {"user": user_from_row(row)},
        }
    except Exception as e:
        return internal_error(e)
