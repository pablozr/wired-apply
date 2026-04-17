import asyncio
from datetime import datetime, timezone

from core.config.config import (
    GLOBAL_CATALOG_CLEANUP_INTERVAL_SECONDS,
    GLOBAL_CATALOG_CLEANUP_SCHEDULE_HOUR_UTC,
)
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.redis.redis import redis_cache
from services.cache import cache_service
from services.pipeline import global_catalog_cleanup_run_service


SCHEDULER_TRIGGER_KEY_PREFIX = "global_catalog:cleanup:trigger"
SCHEDULER_TRIGGER_TTL_SECONDS = 172800


def _scheduler_trigger_key(trigger_date: str) -> str:
    return f"{SCHEDULER_TRIGGER_KEY_PREFIX}:{trigger_date}"


async def _run_cleanup() -> dict:
    async with postgresql.pool.acquire() as conn:
        return await global_catalog_cleanup_run_service.run_global_catalog_cleanup(
            conn,
            redis_cache.redis,
            trigger="scheduler",
        )


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
