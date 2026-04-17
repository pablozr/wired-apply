import asyncio
import uuid
from datetime import datetime, timezone

from core.config.config import (
    GLOBAL_CATALOG_CLEANUP_BATCH_SIZE,
    GLOBAL_CATALOG_CLEANUP_INTERVAL_SECONDS,
    GLOBAL_CATALOG_CLEANUP_LAST_RUN_KEY_PREFIX,
    GLOBAL_CATALOG_CLEANUP_LAST_RUN_TTL_SECONDS,
    GLOBAL_CATALOG_CLEANUP_LOCK_KEY_PREFIX,
    GLOBAL_CATALOG_CLEANUP_SCHEDULE_HOUR_UTC,
    GLOBAL_CATALOG_RETENTION_DAYS,
)
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.redis.redis import redis_cache
from services.cache import cache_service
from services.jobs import global_catalog_cleanup_service


SCHEDULER_TRIGGER_KEY_PREFIX = "global_catalog:cleanup:trigger"
SCHEDULER_TRIGGER_TTL_SECONDS = 172800


def _cleanup_lock_key() -> str:
    return GLOBAL_CATALOG_CLEANUP_LOCK_KEY_PREFIX


def _cleanup_last_run_key() -> str:
    return GLOBAL_CATALOG_CLEANUP_LAST_RUN_KEY_PREFIX


def _scheduler_trigger_key(trigger_date: str) -> str:
    return f"{SCHEDULER_TRIGGER_KEY_PREFIX}:{trigger_date}"


async def _run_cleanup() -> dict:
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
        redis_cache.redis,
    )

    if not acquired:
        return {
            "status": False,
            "message": "Global catalog cleanup already in progress",
            "data": {},
        }

    deleted_jobs_total = 0

    try:
        async with postgresql.pool.acquire() as conn:
            while True:
                batch_result = await global_catalog_cleanup_service.delete_stale_global_jobs_batch(
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
            redis_cache.redis,
        )

        logger.info(
            "global_catalog_cleanup_completed run_id=%s retention_days=%s deleted_jobs=%s",
            run_id,
            safe_retention_days,
            deleted_jobs_total,
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
                "startedAt": started_at,
                "completedAt": datetime.now(timezone.utc).isoformat(),
                "retentionDays": safe_retention_days,
                "batchSize": safe_batch_size,
                "deletedJobs": deleted_jobs_total,
                "error": str(error),
            },
            redis_cache.redis,
        )

        return {
            "status": False,
            "message": "Global catalog cleanup failed",
            "data": {},
        }
    finally:
        try:
            await cache_service.release_lock(lock_key, run_id, redis_cache.redis)
        except Exception as release_error:
            logger.exception(release_error)


async def _tick_scheduler() -> None:
    safe_hour = max(0, min(23, int(GLOBAL_CATALOG_CLEANUP_SCHEDULE_HOUR_UTC)))
    now_utc = datetime.now(timezone.utc)
    if now_utc.hour != safe_hour:
        return

    trigger_date = now_utc.date().isoformat()
    trigger_key = _scheduler_trigger_key(trigger_date)
    should_trigger = await cache_service.acquire_lock(
        trigger_key,
        trigger_date,
        SCHEDULER_TRIGGER_TTL_SECONDS,
        redis_cache.redis,
        fail_open=True,
    )

    if not should_trigger:
        return

    result = await _run_cleanup()
    message = str(result.get("message") or "")
    should_keep_marker = bool(result.get("status")) or (
        "already in progress" in message.lower()
    )

    if not should_keep_marker:
        await cache_service.delete_by_key(trigger_key, redis_cache.redis)

    logger.info(
        "global_catalog_cleanup_scheduler_tick trigger_date=%s status=%s message=%s",
        trigger_date,
        result.get("status"),
        message,
    )


async def run() -> None:
    await postgresql.connect()
    await redis_cache.connect()

    interval_seconds = max(60, int(GLOBAL_CATALOG_CLEANUP_INTERVAL_SECONDS))

    try:
        while True:
            await _tick_scheduler()
            await asyncio.sleep(interval_seconds)
    finally:
        await redis_cache.disconnect()
        await postgresql.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
