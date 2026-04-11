import asyncpg
from asyncpg.exceptions import UniqueViolationError

from core.logger.logger import logger
from schemas.applications import (
    ApplicationCreateRequest,
    ApplicationUpdateRequest,
    application_from_row,
)


async def create_application(
    conn: asyncpg.Connection,
    user_id: int,
    data: ApplicationCreateRequest,
) -> dict:
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO applications (
                user_id,
                job_id,
                status,
                channel,
                notes
            )
            SELECT
                $1,
                j.id,
                $3,
                $4,
                $5
            FROM jobs j
            WHERE j.id = $2 AND j.user_id = $1
            RETURNING
                id,
                user_id,
                job_id,
                status,
                channel,
                notes,
                applied_at,
                created_at,
                updated_at
            """,
            user_id,
            data.job_id,
            data.status,
            data.channel,
            data.notes,
        )

        if not row:
            return {
                "status": False,
                "message": "Job not found for this user",
                "data": {},
            }

        return {
            "status": True,
            "message": "Application created successfully",
            "data": {"application": application_from_row(row)},
        }
    except UniqueViolationError:
        return {
            "status": False,
            "message": "Application already exists for this job",
            "data": {},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def list_applications(
    conn: asyncpg.Connection,
    user_id: int,
    limit: int = 20,
    offset: int = 0,
    status: str | None = None,
) -> dict:
    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)
    normalized_status = status.strip().upper() if status else None

    try:
        if normalized_status:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    user_id,
                    job_id,
                    status,
                    channel,
                    notes,
                    applied_at,
                    created_at,
                    updated_at
                FROM applications
                WHERE user_id = $1 AND status = $2
                ORDER BY created_at DESC
                LIMIT $3 OFFSET $4
                """,
                user_id,
                normalized_status,
                safe_limit,
                safe_offset,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    user_id,
                    job_id,
                    status,
                    channel,
                    notes,
                    applied_at,
                    created_at,
                    updated_at
                FROM applications
                WHERE user_id = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                """,
                user_id,
                safe_limit,
                safe_offset,
            )

        applications = [application_from_row(row) for row in rows]

        return {
            "status": True,
            "message": "Applications retrieved successfully",
            "data": {
                "applications": applications,
                "pagination": {
                    "limit": safe_limit,
                    "offset": safe_offset,
                    "count": len(applications),
                },
            },
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_one_application(
    conn: asyncpg.Connection,
    user_id: int,
    application_id: int,
) -> dict:
    try:
        row = await conn.fetchrow(
            """
            SELECT
                id,
                user_id,
                job_id,
                status,
                channel,
                notes,
                applied_at,
                created_at,
                updated_at
            FROM applications
            WHERE id = $1 AND user_id = $2
            """,
            application_id,
            user_id,
        )

        if not row:
            return {"status": False, "message": "Application not found", "data": {}}

        return {
            "status": True,
            "message": "Application retrieved successfully",
            "data": {"application": application_from_row(row)},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def update_application(
    conn: asyncpg.Connection,
    user_id: int,
    application_id: int,
    data: ApplicationUpdateRequest,
) -> dict:
    allowed_columns = {"status", "notes"}
    filtered = {
        key: value
        for key, value in data.model_dump(exclude_none=True).items()
        if key in allowed_columns
    }

    if not filtered:
        return {"status": False, "message": "No fields to update", "data": {}}

    try:
        columns = list(filtered.keys())
        values = list(filtered.values())
        set_clause = ", ".join(f"{col} = ${idx}" for idx, col in enumerate(columns, 1))

        values.extend([application_id, user_id])

        query = f"""
            UPDATE applications SET {set_clause}, updated_at = NOW()
            WHERE id = ${len(values) - 1} AND user_id = ${len(values)}
            RETURNING
                id,
                user_id,
                job_id,
                status,
                channel,
                notes,
                applied_at,
                created_at,
                updated_at
        """

        row = await conn.fetchrow(query, *values)

        if not row:
            return {"status": False, "message": "Application not found", "data": {}}

        return {
            "status": True,
            "message": "Application updated successfully",
            "data": {"application": application_from_row(row)},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def delete_application(
    conn: asyncpg.Connection,
    user_id: int,
    application_id: int,
) -> dict:
    try:
        result = await conn.execute(
            "DELETE FROM applications WHERE id = $1 AND user_id = $2",
            application_id,
            user_id,
        )

        if result == "DELETE 0":
            return {"status": False, "message": "Application not found", "data": {}}

        return {
            "status": True,
            "message": "Application deleted successfully",
            "data": {"applicationId": application_id},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "Applications module base ready",
        "data": {"module": "applications"},
    }
