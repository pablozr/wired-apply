import asyncio
from datetime import datetime, timezone

from core.config.config import (
    GLOBAL_INGESTION_DEFAULT_DAYS_RANGE,
    GLOBAL_INGESTION_SCHEDULE_HOUR_UTC,
    GLOBAL_INGESTION_SCHEDULER_INTERVAL_SECONDS,
)
from core.logger.logger import logger
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from schemas.pipeline import GlobalIngestionStartRequest
from services.cache import cache_service
from services.pipeline import global_ingestion_service
from workers.common import managed_worker_resources


SCHEDULER_TRIGGER_KEY_PREFIX = "global_ingestion:scheduler:trigger"
SCHEDULER_TRIGGER_TTL_SECONDS = 172800


def _scheduler_trigger_key(trigger_date: str) -> str:
    return f"{SCHEDULER_TRIGGER_KEY_PREFIX}:{trigger_date}"


async def _tick_scheduler() -> None:
    assert rabbitmq.channel is not None

    safe_hour = max(0, min(23, int(GLOBAL_INGESTION_SCHEDULE_HOUR_UTC)))
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

    result = await global_ingestion_service.start_global_ingestion_run(
        rabbitmq.channel,
        GlobalIngestionStartRequest(
            force=False,
            daysRange=GLOBAL_INGESTION_DEFAULT_DAYS_RANGE,
        ),
        redis_cache.redis,
        requested_by_user_id=None,
    )

    message = str(result.get("message") or "")
    should_keep_marker = bool(result.get("status")) or (
        "already in progress" in message.lower()
    )

    if not should_keep_marker:
        await cache_service.delete_by_key(trigger_key, redis_cache.redis)

    logger.info(
        "global_ingestion_scheduler_tick trigger_date=%s status=%s message=%s",
        trigger_date,
        result.get("status"),
        message,
    )


async def run() -> None:
    async with managed_worker_resources(use_redis=True, use_rabbitmq=True):
        interval_seconds = max(60, int(GLOBAL_INGESTION_SCHEDULER_INTERVAL_SECONDS))

        while True:
            await _tick_scheduler()
            await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(run())
