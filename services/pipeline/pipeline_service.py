import uuid

import aio_pika
import asyncpg

from core.config.config import INGESTION_JOBS_QUEUE
from core.logger.logger import logger
from schemas.pipeline import PipelineStartRequest
from services.messaging import messaging_service


async def start_pipeline_run(
    channel: aio_pika.abc.AbstractChannel,
    user_id: int,
    data: PipelineStartRequest,
) -> dict:
    try:
        run_id = str(uuid.uuid4())

        await messaging_service.publish(
            INGESTION_JOBS_QUEUE,
            {
                "event_id": str(uuid.uuid4()),
                "event_version": 1,
                "run_id": run_id,
                "user_id": user_id,
                "force": data.force,
            },
            channel,
        )

        return {
            "status": True,
            "message": "Pipeline run queued",
            "data": {"runId": run_id},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_pipeline_status(conn: asyncpg.Connection, user_id: int) -> dict:
    try:
        jobs_count = await conn.fetchval(
            "SELECT COUNT(*) FROM jobs WHERE user_id = $1", user_id
        )
        applications_count = await conn.fetchval(
            "SELECT COUNT(*) FROM applications WHERE user_id = $1", user_id
        )

        return {
            "status": True,
            "message": "Pipeline status retrieved successfully",
            "data": {
                "jobsCount": int(jobs_count or 0),
                "applicationsCount": int(applications_count or 0),
            },
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}
