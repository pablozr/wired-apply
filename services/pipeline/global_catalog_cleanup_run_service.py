import uuid
from datetime import datetime, timezone

import asyncpg

from core.config.config import (
    GLOBAL_CATALOG_CLEANUP_BATCH_SIZE,
    GLOBAL_CATALOG_CLEANUP_INTERVAL_SECONDS,
    GLOBAL_CATALOG_CLEANUP_LAST_RUN_KEY_PREFIX,
    GLOBAL_CATALOG_CLEANUP_LAST_RUN_TTL_SECONDS,
    GLOBAL_CATALOG_CLEANUP_LOCK_KEY_PREFIX,
    GLOBAL_CATALOG_RETENTION_DAYS,
)
from core.logger.logger import logger
from services.cache import cache_service
from services.jobs import global_catalog_cleanup_service as cleanup_jobs_service


def _cleanup_lock_key() -> str:
    return GLOBAL_CATALOG_CLEANUP_LOCK_KEY_PREFIX


def _cleanup_last_run_key() -> str:
    return GLOBAL_CATALOG_CLEANUP_LAST_RUN_KEY_PREFIX


async def run_global_catalog_cleanup(
    conn: asyncpg.Connection,
    redis_client,
    requested_by_user_id: int | None = None,
    trigger: str = "manual",
) -> dict:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    lock_key = _cleanup_lock_key()
    last_run_key = _cleanup_last_run_key()

    safe_retention_days = max(1, min(int(GLOBAL_CATALOG_RETENTION_DAYS), 3650))
    safe_batch_size = max(1, min(int(GLOBAL_CATALOG_CLEANUP_BATCH_SIZE), 10000))
    lock_ttl_seconds = max(300, int(GLOBAL_CATALOG_CLEANUP_INTERVAL_SECONDS) * 2)

    acquired = await cache_service.acquire_lock(
        lock_key,
        run_id,
        lock_ttl_seconds,
        redis_client,
    )

    if not acquired:
        return {
            "status": False,
            "message": "Global catalog cleanup already in progress",
            "data": {},
        }

    deleted_jobs_total = 0

    try:
        while True:
            batch_result = await cleanup_jobs_service.delete_stale_global_jobs_batch(
                conn,
                safe_retention_days,
                safe_batch_size,
            )

            if not batch_result.get("status"):
                raise RuntimeError(batch_result.get("message") or "Cleanup batch failed")

            deleted_jobs = int(batch_result.get("data", {}).get("deletedJobs", 0))
            deleted_jobs_total += deleted_jobs

            if deleted_jobs < safe_batch_size:
                break

        completed_at = datetime.now(timezone.utc).isoformat()
        payload = {
            "runId": run_id,
            "status": "COMPLETED",
            "trigger": trigger,
            "requestedByUserId": requested_by_user_id,
            "startedAt": started_at,
            "completedAt": completed_at,
            "retentionDays": safe_retention_days,
            "batchSize": safe_batch_size,
            "deletedJobs": deleted_jobs_total,
        }

        await cache_service.set_by_key(
            last_run_key,
            GLOBAL_CATALOG_CLEANUP_LAST_RUN_TTL_SECONDS,
            payload,
            redis_client,
        )

        logger.info(
            "global_catalog_cleanup_completed run_id=%s trigger=%s retention_days=%s deleted_jobs=%s requested_by_user_id=%s",
            run_id,
            trigger,
            safe_retention_days,
            deleted_jobs_total,
            requested_by_user_id,
        )

        return {
            "status": True,
            "message": "Global catalog cleanup completed",
            "data": payload,
        }
    except Exception as error:
        logger.exception(error)

        await cache_service.set_by_key(
            last_run_key,
            GLOBAL_CATALOG_CLEANUP_LAST_RUN_TTL_SECONDS,
            {
                "runId": run_id,
                "status": "FAILED",
                "trigger": trigger,
                "requestedByUserId": requested_by_user_id,
                "startedAt": started_at,
                "completedAt": datetime.now(timezone.utc).isoformat(),
                "retentionDays": safe_retention_days,
                "batchSize": safe_batch_size,
                "deletedJobs": deleted_jobs_total,
                "error": str(error),
            },
            redis_client,
        )

        return {
            "status": False,
            "message": "Global catalog cleanup failed",
            "data": {},
        }
    finally:
        try:
            await cache_service.release_lock(lock_key, run_id, redis_client)
        except Exception as release_error:
            logger.exception(release_error)