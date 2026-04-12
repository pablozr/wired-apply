import asyncio
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import (
    PIPELINE_EVENT_DEDUPE_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
    PIPELINE_RETRY_BASE_DELAY_SECONDS,
    PIPELINE_RETRY_MAX_ATTEMPTS,
    RETRY_APPLY_QUEUE,
    SHORTLIST_APPLY_QUEUE,
)
from core.logger.logger import logger
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from services.cache import cache_service
from services.messaging import messaging_service


def _event_dedupe_key(event_id: str) -> str:
    return f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


def _backoff_seconds(retry_count: int) -> int:
    exponent = max(0, retry_count - 1)
    return PIPELINE_RETRY_BASE_DELAY_SECONDS * (2**exponent)


async def process_retry_event(message: AbstractIncomingMessage) -> None:
    async with message.process():
        payload = json.loads(message.body.decode())
        event_id = payload.get("event_id")
        run_id = payload.get("run_id")
        user_id = payload.get("user_id")
        job_id = payload.get("job_id")
        retry_count = int(payload.get("retry_count") or 0)

        if not event_id or not run_id or not user_id or not job_id:
            logger.error("retry_worker_invalid_event payload=%s", payload)
            return

        dedupe_key = _event_dedupe_key(str(event_id))
        is_new_event = await cache_service.acquire_lock(
            dedupe_key,
            str(event_id),
            PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
            redis_cache.redis,
            fail_open=True,
        )

        if not is_new_event:
            logger.info("retry_worker_duplicate_event event_id=%s", event_id)
            return

        if retry_count > PIPELINE_RETRY_MAX_ATTEMPTS:
            logger.error(
                "retry_worker_max_attempts_reached run_id=%s user_id=%s job_id=%s attempts=%s",
                run_id,
                user_id,
                job_id,
                retry_count,
            )
            return

        delay_seconds = _backoff_seconds(retry_count)
        await asyncio.sleep(delay_seconds)

        await messaging_service.publish(
            SHORTLIST_APPLY_QUEUE,
            {
                "event_id": str(uuid.uuid4()),
                "event_version": 1,
                "run_id": run_id,
                "user_id": user_id,
                "job_id": job_id,
                "retry_count": retry_count,
            },
            rabbitmq.channel,
        )

        logger.info(
            "retry_worker_requeued_apply run_id=%s user_id=%s job_id=%s retry_count=%s delay_seconds=%s",
            run_id,
            user_id,
            job_id,
            retry_count,
            delay_seconds,
        )


async def run() -> None:
    await redis_cache.connect()
    await rabbitmq.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(RETRY_APPLY_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(SHORTLIST_APPLY_QUEUE, durable=True)
    await queue.consume(process_retry_event)

    try:
        await asyncio.Future()
    finally:
        await rabbitmq.disconnect()
        await redis_cache.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
