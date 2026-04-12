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
from core.logger.logger import logger
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from services.cache import cache_service
from services.messaging import messaging_service


def _event_dedupe_key(event_id: str) -> str:
    return f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


def _build_mock_jobs(force: bool) -> list[dict]:
    jobs = [
        {
            "title": "Backend Engineer",
            "company": "Acme Labs",
            "location": "Remote",
            "source": "ingestion",
            "source_url": "https://jobs.example.com/backend-engineer",
            "external_job_id": "wa-backend-engineer",
        },
        {
            "title": "Python Developer",
            "company": "Orbit Systems",
            "location": "Sao Paulo",
            "source": "ingestion",
            "source_url": "https://jobs.example.com/python-developer",
            "external_job_id": "wa-python-developer",
        },
        {
            "title": "Data Engineer",
            "company": "Nova Data",
            "location": "Remote",
            "source": "ingestion",
            "source_url": "https://jobs.example.com/data-engineer",
            "external_job_id": "wa-data-engineer",
        },
    ]

    if force:
        jobs.append(
            {
                "title": "Site Reliability Engineer",
                "company": "Atlas Cloud",
                "location": "Remote",
                "source": "ingestion",
                "source_url": "https://jobs.example.com/sre",
                "external_job_id": "wa-sre-engineer",
            }
        )

    return jobs


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

        jobs = _build_mock_jobs(force)
        total_jobs = len(jobs)

        for index, raw_job in enumerate(jobs, start=1):
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
            "ingestion_worker_queued_normalization run_id=%s user_id=%s total_jobs=%s",
            run_id,
            user_id,
            total_jobs,
        )


async def run() -> None:
    await redis_cache.connect()
    await rabbitmq.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(INGESTION_JOBS_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(JOBS_NORMALIZED_QUEUE, durable=True)
    await queue.consume(process_ingestion_event)

    try:
        await asyncio.Future()
    finally:
        await rabbitmq.disconnect()
        await redis_cache.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
