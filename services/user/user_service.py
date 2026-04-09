import asyncpg
from asyncpg.exceptions import UniqueViolationError

from core.logger.logger import logger
from core.security.hashing import hash_password
from schemas.user import UserCreateRequest, UserUpdateRequest, user_from_row


async def get_one_user(conn: asyncpg.Connection, user_id: int) -> dict:
    try:
        row = await conn.fetchrow(
            "SELECT id, fullname, email, role, created_at FROM users WHERE id = $1",
            user_id,
        )

        if not row:
            return {"status": False, "message": "User not found", "data": {}}

        return {
            "status": True,
            "message": "User retrieved successfully",
            "data": {"user": user_from_row(row)},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def create(conn: asyncpg.Connection, data: UserCreateRequest) -> dict:
    try:
        hashed = hash_password(data.password)

        row = await conn.fetchrow(
            """
            INSERT INTO users (fullname, email, password, role)
            VALUES ($1, $2, $3, 'BASIC')
            RETURNING id, fullname, email, role, created_at
            """,
            data.fullname,
            data.email,
            hashed,
        )

        return {
            "status": True,
            "message": "User created successfully",
            "data": {"user": user_from_row(row)},
        }
    except UniqueViolationError:
        return {"status": False, "message": "Email already registered", "data": {}}
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def update_me(
    conn: asyncpg.Connection, user_id: int, data: UserUpdateRequest
) -> dict:
    allowed_columns = {"fullname", "email"}
    filtered = {
        k: v
        for k, v in data.model_dump(exclude_none=True).items()
        if k in allowed_columns
    }

    if not filtered:
        return {"status": False, "message": "No fields to update", "data": {}}

    try:
        columns = list(filtered.keys())
        values = list(filtered.values())
        set_clause = ", ".join(f"{col} = ${i}" for i, col in enumerate(columns, 1))
        values.append(user_id)

        query = f"""
            UPDATE users SET {set_clause}, updated_at = NOW()
            WHERE id = ${len(values)}
            RETURNING id, fullname, email, role, created_at
        """
        row = await conn.fetchrow(query, *values)

        if not row:
            return {"status": False, "message": "User not found", "data": {}}

        return {
            "status": True,
            "message": "User updated successfully",
            "data": {"user": user_from_row(row)},
        }
    except UniqueViolationError:
        return {"status": False, "message": "Email already in use", "data": {}}
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}
