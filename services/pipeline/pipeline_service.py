import uuid
from datetime import datetime, timezone

import aio_pika
import asyncpg

from core.config.config import (
    INGESTION_JOBS_QUEUE,
    PIPELINE_LAST_RUN_KEY_PREFIX,
    PIPELINE_LAST_RUN_TTL_SECONDS,
    PIPELINE_RUN_LOCK_KEY_PREFIX,
    PIPELINE_RUN_LOCK_TTL_SECONDS,
)
from core.logger.logger import logger
from schemas.pipeline import PipelineStartRequest
from services.cache import cache_service
from services.messaging import messaging_service


def _pipeline_lock_key(user_id: int) -> str:
    return f"{PIPELINE_RUN_LOCK_KEY_PREFIX}:{user_id}"


def _pipeline_last_run_key(user_id: int) -> str:
    return f"{PIPELINE_LAST_RUN_KEY_PREFIX}:{user_id}"


async def start_pipeline_run(
    channel: aio_pika.abc.AbstractChannel,
    user_id: int,
    data: PipelineStartRequest,
    redis_client,
) -> dict:
    run_id = str(uuid.uuid4())
    queued_at = datetime.now(timezone.utc).isoformat()
    date_from, date_to = data.resolve_window()
    date_from_iso = date_from.isoformat()
    date_to_iso = date_to.isoformat()
    days_range = ((date_to - date_from).days + 1)
    force_rescore = bool(data.force_rescore)
    lock_key = _pipeline_lock_key(user_id)

    try:
        acquired = await cache_service.acquire_lock(
            lock_key,
            run_id,
            PIPELINE_RUN_LOCK_TTL_SECONDS,
            redis_client,
        )

        if not acquired:
            return {
                "status": False,
                "message": "Pipeline run already in progress for this user",
                "data": {},
            }

        await messaging_service.publish(
            INGESTION_JOBS_QUEUE,
            {
                "event_id": str(uuid.uuid4()),
                "event_version": 1,
                "run_id": run_id,
                "user_id": user_id,
                "force": data.force,
                "force_rescore": force_rescore,
                "date_from": date_from_iso,
                "date_to": date_to_iso,
                "days_range": days_range,
                "queued_at": queued_at,
            },
            channel,
        )

        try:
            await cache_service.set_by_key(
                _pipeline_last_run_key(user_id),
                PIPELINE_LAST_RUN_TTL_SECONDS,
                {
                    "runId": run_id,
                    "status": "QUEUED",
                    "queuedAt": queued_at,
                    "force": data.force,
                    "forceRescore": force_rescore,
                    "dateFrom": date_from_iso,
                    "dateTo": date_to_iso,
                    "daysRange": days_range,
                },
                redis_client,
            )
        except Exception as metadata_error:
            logger.exception(metadata_error)

        return {
            "status": True,
            "message": "Pipeline run queued",
            "data": {
                "runId": run_id,
                "lockTtlSeconds": PIPELINE_RUN_LOCK_TTL_SECONDS,
                "dateFrom": date_from_iso,
                "dateTo": date_to_iso,
                "daysRange": days_range,
                "forceRescore": force_rescore,
            },
        }
    except Exception as e:
        try:
            await cache_service.release_lock(lock_key, run_id, redis_client)
        except Exception as release_error:
            logger.exception(release_error)

        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_pipeline_status(conn: asyncpg.Connection, user_id: int, redis_client) -> dict:
    try:
        jobs_count = await conn.fetchval(
            "SELECT COUNT(*) FROM jobs WHERE user_id = $1", user_id
        )
        applications_count = await conn.fetchval(
            "SELECT COUNT(*) FROM applications WHERE user_id = $1", user_id
        )
        lock_key = _pipeline_lock_key(user_id)
        active_run = await cache_service.get_by_key(lock_key, redis_client)
        active_run_ttl = await cache_service.get_ttl(lock_key, redis_client)
        last_run = await cache_service.get_by_key(
            _pipeline_last_run_key(user_id),
            redis_client,
        )

        active_run_id = active_run if isinstance(active_run, str) else None
        last_run_data = last_run if isinstance(last_run, dict) else None

        return {
            "status": True,
            "message": "Pipeline status retrieved successfully",
            "data": {
                "jobsCount": int(jobs_count or 0),
                "applicationsCount": int(applications_count or 0),
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
