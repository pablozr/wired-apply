import asyncio
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import (
    INGESTION_JOBS_QUEUE,
    JOBS_NORMALIZED_QUEUE,
    PIPELINE_EVENT_DEDUPE_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
)
from core.http.http_client import http_client
from core.logger.logger import logger
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from services.cache import cache_service
from services.integrations import ats_service
from services.messaging import messaging_service


def _event_dedupe_key(event_id: str) -> str:
    return f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


async def process_ingestion_event(message: AbstractIncomingMessage) -> None:
    async with message.process():
        payload = json.loads(message.body.decode())
        event_id = payload.get("event_id")
        run_id = payload.get("run_id")
        user_id = payload.get("user_id")
        force = bool(payload.get("force", False))

        if not event_id or not run_id or not user_id:
            logger.error("ingestion_worker_invalid_event payload=%s", payload)
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
            logger.info("ingestion_worker_duplicate_event event_id=%s", event_id)
            return

        jobs_result = await ats_service.fetch_jobs(force=force)
        if not jobs_result["status"]:
            logger.error(
                "ingestion_worker_ats_fetch_failed run_id=%s user_id=%s message=%s",
                run_id,
                user_id,
                jobs_result["message"],
            )
            return

        jobs_data = jobs_result.get("data", {})
        jobs = jobs_data.get("jobs", [])
        if not isinstance(jobs, list):
            logger.error(
                "ingestion_worker_ats_invalid_payload run_id=%s user_id=%s",
                run_id,
                user_id,
            )
            return

        raw_jobs = [job for job in jobs if isinstance(job, dict)]
        if not raw_jobs:
            logger.warning(
                "ingestion_worker_no_jobs_to_publish run_id=%s user_id=%s",
                run_id,
                user_id,
            )
            return

        total_jobs = len(raw_jobs)
        fallback_used = bool(jobs_data.get("fallbackUsed", False))
        source_count = len(jobs_data.get("sources", []))

        for index, raw_job in enumerate(raw_jobs, start=1):
            await messaging_service.publish(
                JOBS_NORMALIZED_QUEUE,
                {
                    "event_id": str(uuid.uuid4()),
                    "event_version": 1,
                    "run_id": run_id,
                    "user_id": user_id,
                    "sequence": index,
                    "total_jobs": total_jobs,
                    "raw_job": raw_job,
                },
                rabbitmq.channel,
            )

        logger.info(
            "ingestion_worker_queued_normalization run_id=%s user_id=%s total_jobs=%s sources=%s fallback=%s",
            run_id,
            user_id,
            total_jobs,
            source_count,
            fallback_used,
        )


async def run() -> None:
    await redis_cache.connect()
    await rabbitmq.connect()
    await http_client.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(INGESTION_JOBS_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(JOBS_NORMALIZED_QUEUE, durable=True)
    await queue.consume(process_ingestion_event)

    try:
        await asyncio.Future()
    finally:
        await http_client.disconnect()
        await rabbitmq.disconnect()
        await redis_cache.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
