import uuid
from datetime import datetime, timezone

import aio_pika

from core.config.config import (
    GLOBAL_INGESTION_JOBS_QUEUE,
    GLOBAL_INGESTION_LAST_RUN_KEY_PREFIX,
    GLOBAL_INGESTION_LAST_RUN_TTL_SECONDS,
    GLOBAL_INGESTION_RUN_LOCK_KEY_PREFIX,
    GLOBAL_INGESTION_RUN_LOCK_TTL_SECONDS,
)
from core.logger.logger import logger
from schemas.pipeline import GlobalIngestionStartRequest
from services.cache import cache_service
from services.messaging import messaging_service


def _global_ingestion_lock_key() -> str:
    return GLOBAL_INGESTION_RUN_LOCK_KEY_PREFIX


def _global_ingestion_last_run_key() -> str:
    return GLOBAL_INGESTION_LAST_RUN_KEY_PREFIX


async def start_global_ingestion_run(
    channel: aio_pika.abc.AbstractChannel,
    data: GlobalIngestionStartRequest,
    redis_client,
    requested_by_user_id: int | None = None,
) -> dict:
    run_id = str(uuid.uuid4())
    queued_at = datetime.now(timezone.utc).isoformat()
    lock_key = _global_ingestion_lock_key()

    try:
        acquired = await cache_service.acquire_lock(
            lock_key,
            run_id,
            GLOBAL_INGESTION_RUN_LOCK_TTL_SECONDS,
            redis_client,
        )

        if not acquired:
            return {
                "status": False,
                "message": "Global ingestion run already in progress",
                "data": {},
            }

        await messaging_service.publish(
            GLOBAL_INGESTION_JOBS_QUEUE,
            {
                "event_id": str(uuid.uuid4()),
                "event_version": 1,
                "run_id": run_id,
                "force": bool(data.force),
                "requested_by_user_id": requested_by_user_id,
                "queued_at": queued_at,
            },
            channel,
        )

        try:
            await cache_service.set_by_key(
                _global_ingestion_last_run_key(),
                GLOBAL_INGESTION_LAST_RUN_TTL_SECONDS,
                {
                    "runId": run_id,
                    "status": "QUEUED",
                    "queuedAt": queued_at,
                    "force": bool(data.force),
                    "requestedByUserId": requested_by_user_id,
                },
                redis_client,
            )
        except Exception as metadata_error:
            logger.exception(metadata_error)

        return {
            "status": True,
            "message": "Global ingestion run queued",
            "data": {
                "runId": run_id,
                "lockTtlSeconds": GLOBAL_INGESTION_RUN_LOCK_TTL_SECONDS,
            },
        }
    except Exception as e:
        try:
            await cache_service.release_lock(lock_key, run_id, redis_client)
        except Exception as release_error:
            logger.exception(release_error)

        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_global_ingestion_status(redis_client) -> dict:
    try:
        lock_key = _global_ingestion_lock_key()
        active_run = await cache_service.get_by_key(lock_key, redis_client)
        active_run_ttl = await cache_service.get_ttl(lock_key, redis_client)
        last_run = await cache_service.get_by_key(
            _global_ingestion_last_run_key(),
            redis_client,
        )

        active_run_id = active_run if isinstance(active_run, str) else None
        last_run_data = last_run if isinstance(last_run, dict) else None

        return {
            "status": True,
            "message": "Global ingestion status retrieved successfully",
            "data": {
                "isRunning": bool(active_run_id),
                "activeRunId": active_run_id,
                "activeRunTtlSeconds": (
                    active_run_ttl if active_run_ttl > 0 else None
                ),
                "lastRun": last_run_data,
            },
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}