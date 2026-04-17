from datetime import datetime, timedelta

import asyncpg

from core.logger.logger import logger


async def delete_stale_global_jobs_batch(
    conn: asyncpg.Connection,
    retention_days: int = 45,
    batch_size: int = 1000,
) -> dict:
    safe_retention_days = max(1, min(int(retention_days), 3650))
    safe_batch_size = max(1, min(int(batch_size), 10000))

    try:
        cutoff_timestamp = datetime.utcnow() - timedelta(days=safe_retention_days)

        rows = await conn.fetch(
            """
            WITH stale_jobs AS (
                SELECT id
                FROM global_jobs
                WHERE last_seen_at < $1
                ORDER BY last_seen_at ASC
                LIMIT $2
            )
            DELETE FROM global_jobs gj
            USING stale_jobs sj
            WHERE gj.id = sj.id
            RETURNING gj.id
            """,
            cutoff_timestamp,
            safe_batch_size,
        )

        deleted_jobs = len(rows)

        return {
            "status": True,
            "message": "Global catalog cleanup batch completed",
            "data": {
                "deletedJobs": deleted_jobs,
                "retentionDays": safe_retention_days,
                "batchSize": safe_batch_size,
                "cutoffAt": cutoff_timestamp.isoformat(),
            },
        }
    except Exception as e:
        logger.exception(e)
        return {
            "status": False,
            "message": "Global catalog cleanup batch failed",
            "data": {},
        }
