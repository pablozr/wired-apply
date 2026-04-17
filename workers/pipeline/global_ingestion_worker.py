import asyncio
import json
from datetime import datetime, timezone

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import (
    GLOBAL_INGESTION_EVENT_DEDUPE_KEY_PREFIX,
    GLOBAL_INGESTION_JOBS_QUEUE,
    GLOBAL_INGESTION_LAST_RUN_KEY_PREFIX,
    GLOBAL_INGESTION_LAST_RUN_TTL_SECONDS,
    GLOBAL_INGESTION_RUN_LOCK_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
)
from core.http.http_client import http_client
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from services.cache import cache_service
from services.integrations import ats_service
from services.jobs import global_jobs_service


def _event_dedupe_key(event_id: str) -> str:
    return f"{GLOBAL_INGESTION_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


def _global_ingestion_lock_key() -> str:
    return GLOBAL_INGESTION_RUN_LOCK_KEY_PREFIX


def _global_ingestion_last_run_key() -> str:
    return GLOBAL_INGESTION_LAST_RUN_KEY_PREFIX


async def process_global_ingestion_event(message: AbstractIncomingMessage) -> None:
    async with message.process():
        payload = json.loads(message.body.decode())
        event_id = payload.get("event_id")
        run_id = payload.get("run_id")
        force = bool(payload.get("force", False))
        queued_at = payload.get("queued_at")
        requested_by_user_id = payload.get("requested_by_user_id")

        if not event_id or not run_id:
            logger.error("global_ingestion_worker_invalid_event payload=%s", payload)
            return

        if not isinstance(queued_at, str) or not queued_at.strip():
            queued_at = datetime.now(timezone.utc).isoformat()

        dedupe_key = _event_dedupe_key(str(event_id))
        is_new_event = await cache_service.acquire_lock(
            dedupe_key,
            str(event_id),
            PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
            redis_cache.redis,
            fail_open=True,
        )

        if not is_new_event:
            logger.info("global_ingestion_worker_duplicate_event event_id=%s", event_id)
            return

        lock_key = _global_ingestion_lock_key()
        last_run_key = _global_ingestion_last_run_key()
        persisted_jobs = 0
        failed_jobs = 0

        try:
            jobs_result = await ats_service.fetch_jobs(force=force)
            if not jobs_result.get("status"):
                raise RuntimeError(jobs_result.get("message") or "ATS fetch failed")

            jobs_data = jobs_result.get("data", {})
            jobs = jobs_data.get("jobs", [])
            if not isinstance(jobs, list):
                raise ValueError("ATS fetch returned invalid jobs payload")

            async with postgresql.pool.acquire() as conn:
                for raw_job in jobs:
                    if not isinstance(raw_job, dict):
                        continue

                    try:
                        source_target = raw_job.get("source_target")
                        source_target_value = (
                            str(source_target).strip() if source_target is not None else None
                        )

                        await global_jobs_service.upsert_global_job(
                            conn,
                            raw_job,
                            source_target_value,
                        )
                        persisted_jobs += 1
                    except Exception as upsert_error:
                        failed_jobs += 1
                        logger.warning(
                            "global_ingestion_worker_upsert_failed run_id=%s source=%s external_job_id=%s error=%s",
                            run_id,
                            raw_job.get("source"),
                            raw_job.get("external_job_id"),
                            upsert_error,
                        )

            completed_at = datetime.now(timezone.utc).isoformat()
            total_jobs = len([job for job in jobs if isinstance(job, dict)])

            await cache_service.set_by_key(
                last_run_key,
                GLOBAL_INGESTION_LAST_RUN_TTL_SECONDS,
                {
                    "runId": str(run_id),
                    "status": "COMPLETED",
                    "queuedAt": queued_at,
                    "completedAt": completed_at,
                    "force": force,
                    "requestedByUserId": requested_by_user_id,
                    "totalJobs": total_jobs,
                    "persistedJobs": persisted_jobs,
                    "failedJobs": failed_jobs,
                    "sources": jobs_data.get("sources", []),
                    "fallbackUsed": bool(jobs_data.get("fallbackUsed", False)),
                },
                redis_cache.redis,
            )

            logger.info(
                "global_ingestion_worker_processed run_id=%s total_jobs=%s persisted_jobs=%s failed_jobs=%s fallback=%s",
                run_id,
                total_jobs,
                persisted_jobs,
                failed_jobs,
                bool(jobs_data.get("fallbackUsed", False)),
            )
        except Exception as error:
            logger.exception(error)

            await cache_service.set_by_key(
                last_run_key,
                GLOBAL_INGESTION_LAST_RUN_TTL_SECONDS,
                {
                    "runId": str(run_id),
                    "status": "FAILED",
                    "queuedAt": queued_at,
                    "completedAt": datetime.now(timezone.utc).isoformat(),
                    "force": force,
                    "requestedByUserId": requested_by_user_id,
                    "error": str(error),
                },
                redis_cache.redis,
            )
        finally:
            try:
                await cache_service.release_lock(lock_key, str(run_id), redis_cache.redis)
            except Exception as release_error:
                logger.exception(release_error)


async def run() -> None:
    await postgresql.connect()
    await redis_cache.connect()
    await rabbitmq.connect()
    await http_client.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(GLOBAL_INGESTION_JOBS_QUEUE, durable=True)
    await queue.consume(process_global_ingestion_event)

    try:
        await asyncio.Future()
    finally:
        await http_client.disconnect()
        await rabbitmq.disconnect()
        await redis_cache.disconnect()
        await postgresql.disconnect()


if __name__ == "__main__":
    asyncio.run(run())