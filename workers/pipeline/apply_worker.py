import asyncio
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import (
    PIPELINE_EVENT_DEDUPE_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
    RETRY_APPLY_QUEUE,
    SHORTLIST_APPLY_QUEUE,
)
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from services.cache import cache_service
from services.messaging import messaging_service
from services.rules import application_constraints


def _event_dedupe_key(event_id: str) -> str:
    return f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


async def _publish_retry(payload: dict, reason: str) -> None:
    retry_count = int(payload.get("retry_count") or 0) + 1

    await messaging_service.publish(
        RETRY_APPLY_QUEUE,
        {
            "event_id": str(uuid.uuid4()),
            "event_version": 1,
            "run_id": payload.get("run_id"),
            "user_id": payload.get("user_id"),
            "job_id": payload.get("job_id"),
            "retry_count": retry_count,
            "reason": reason,
        },
        rabbitmq.channel,
    )


async def process_apply_event(message: AbstractIncomingMessage) -> None:
    async with message.process():
        payload = json.loads(message.body.decode())
        event_id = payload.get("event_id")
        run_id = payload.get("run_id")
        user_id = payload.get("user_id")
        job_id = payload.get("job_id")

        if not event_id or not run_id or not user_id or not job_id:
            logger.error("apply_worker_invalid_event payload=%s", payload)
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
            logger.info("apply_worker_duplicate_event event_id=%s", event_id)
            return

        try:
            async with postgresql.pool.acquire() as conn:
                job_row = await conn.fetchrow(
                    """
                    SELECT id, status
                    FROM jobs
                    WHERE id = $1 AND user_id = $2
                    """,
                    int(job_id),
                    int(user_id),
                )

                if not job_row:
                    logger.error(
                        "apply_worker_job_not_found run_id=%s user_id=%s job_id=%s",
                        run_id,
                        user_id,
                        job_id,
                    )
                    return

                job_status = (job_row["status"] or "").strip().upper()
                application_status = (
                    "APPLY_READY"
                    if application_constraints.can_auto_apply(job_status)
                    else "PENDING"
                )

                await conn.execute(
                    """
                    INSERT INTO applications (
                        user_id,
                        job_id,
                        status,
                        channel,
                        notes,
                        applied_at
                    )
                    VALUES ($1, $2, $3, $4, $5, NULL)
                    ON CONFLICT (user_id, job_id)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        channel = EXCLUDED.channel,
                        notes = EXCLUDED.notes,
                        updated_at = NOW()
                    """,
                    int(user_id),
                    int(job_id),
                    application_status,
                    "ASSISTED",
                    "Auto-shortlisted by worker; waiting human confirmation for submit.",
                )

            logger.info(
                "apply_worker_processed run_id=%s user_id=%s job_id=%s status=%s",
                run_id,
                user_id,
                job_id,
                application_status,
            )
        except Exception as e:
            logger.exception(e)
            await _publish_retry(payload, str(e))


async def run() -> None:
    await postgresql.connect()
    await redis_cache.connect()
    await rabbitmq.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(SHORTLIST_APPLY_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(RETRY_APPLY_QUEUE, durable=True)
    await queue.consume(process_apply_event)

    try:
        await asyncio.Future()
    finally:
        await rabbitmq.disconnect()
        await redis_cache.disconnect()
        await postgresql.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
