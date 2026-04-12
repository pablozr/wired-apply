import uuid
from datetime import date, datetime, timezone

import aio_pika
import asyncpg

from core.config.config import DIGEST_EMAIL_QUEUE
from core.logger.logger import logger
from schemas.digest import DigestGenerateRequest, digest_from_row
from services.messaging import messaging_service


async def generate_daily_digest(
    conn: asyncpg.Connection,
    channel: aio_pika.abc.AbstractChannel,
    user_id: int,
    data: DigestGenerateRequest,
) -> dict:
    digest_date = data.digest_date or date.today()

    try:
        async with conn.transaction():
            total_jobs = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM jobs
                WHERE user_id = $1 AND DATE(created_at) = $2
                """,
                user_id,
                digest_date,
            )
            total_applications = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM applications
                WHERE user_id = $1 AND DATE(created_at) = $2
                """,
                user_id,
                digest_date,
            )
            total_interviews = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM applications
                WHERE user_id = $1
                    AND UPPER(status) LIKE 'INTERVIEW%'
                    AND DATE(updated_at) = $2
                """,
                user_id,
                digest_date,
            )

            payload = {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "counts": {
                    "jobs": int(total_jobs or 0),
                    "applications": int(total_applications or 0),
                    "interviews": int(total_interviews or 0),
                },
            }

            row = await conn.fetchrow(
                """
                INSERT INTO daily_digest (
                    user_id,
                    digest_date,
                    total_jobs,
                    total_applications,
                    total_interviews,
                    payload
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (user_id, digest_date)
                DO UPDATE
                SET
                    total_jobs = EXCLUDED.total_jobs,
                    total_applications = EXCLUDED.total_applications,
                    total_interviews = EXCLUDED.total_interviews,
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
                RETURNING
                    id,
                    user_id,
                    digest_date,
                    total_jobs,
                    total_applications,
                    total_interviews,
                    payload,
                    created_at,
                    updated_at
                """,
                user_id,
                digest_date,
                int(total_jobs or 0),
                int(total_applications or 0),
                int(total_interviews or 0),
                payload,
            )

        await messaging_service.publish(
            DIGEST_EMAIL_QUEUE,
            {
                "event_id": str(uuid.uuid4()),
                "event_version": 1,
                "user_id": user_id,
                "digest_id": row["id"],
                "digest_date": str(row["digest_date"]),
            },
            channel,
        )

        return {
            "status": True,
            "message": "Daily digest generated successfully",
            "data": {"digest": digest_from_row(row)},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_daily_digest(
    conn: asyncpg.Connection,
    user_id: int,
    digest_date: date | None = None,
) -> dict:
    try:
        if digest_date is None:
            row = await conn.fetchrow(
                """
                SELECT
                    id,
                    user_id,
                    digest_date,
                    total_jobs,
                    total_applications,
                    total_interviews,
                    payload,
                    created_at,
                    updated_at
                FROM daily_digest
                WHERE user_id = $1
                ORDER BY digest_date DESC
                LIMIT 1
                """,
                user_id,
            )
        else:
            row = await conn.fetchrow(
                """
                SELECT
                    id,
                    user_id,
                    digest_date,
                    total_jobs,
                    total_applications,
                    total_interviews,
                    payload,
                    created_at,
                    updated_at
                FROM daily_digest
                WHERE user_id = $1 AND digest_date = $2
                """,
                user_id,
                digest_date,
            )

        if not row:
            return {"status": False, "message": "Daily digest not found", "data": {}}

        return {
            "status": True,
            "message": "Daily digest retrieved successfully",
            "data": {"digest": digest_from_row(row)},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "Digest module base ready",
        "data": {"module": "digest"},
    }
