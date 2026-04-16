import asyncio
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import (
    JOBS_NORMALIZED_QUEUE,
    PIPELINE_EVENT_DEDUPE_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
    SCORING_JOBS_QUEUE,
)
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from services.cache import cache_service
from services.messaging import messaging_service
from services.rules import deduplication_policy


def _event_dedupe_key(event_id: str) -> str:
    return f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


def _normalize_raw_job(raw_job: dict) -> dict:
    title = (raw_job.get("title") or "Unknown Role").strip()
    company = (raw_job.get("company") or "Unknown Company").strip()
    location = raw_job.get("location")
    source = (raw_job.get("source") or "ingestion").strip().lower()
    source_url = raw_job.get("source_url")
    external_job_id = raw_job.get("external_job_id")
    description = raw_job.get("description")
    requirements = raw_job.get("requirements")
    employment_type = raw_job.get("employment_type")
    seniority_hint = raw_job.get("seniority_hint")
    remote_policy = raw_job.get("remote_policy")
    tech_stack = raw_job.get("tech_stack")
    ingestion_relevance_score = raw_job.get("ingestion_relevance_score")
    ingestion_relevance_reason = raw_job.get("ingestion_relevance_reason")
    ingestion_exploration_kept = raw_job.get("ingestion_exploration_kept")

    if isinstance(location, str):
        location = location.strip() or None
    else:
        location = None

    if isinstance(source_url, str):
        source_url = source_url.strip() or None
    else:
        source_url = None

    if isinstance(external_job_id, str):
        external_job_id = external_job_id.strip().lower() or None
    else:
        external_job_id = None

    if isinstance(description, str):
        description = description.strip() or None
    else:
        description = None

    if isinstance(requirements, str):
        requirements = requirements.strip() or None
    else:
        requirements = None

    if isinstance(employment_type, str):
        employment_type = employment_type.strip() or None
    else:
        employment_type = None

    if isinstance(seniority_hint, str):
        seniority_hint = seniority_hint.strip() or None
    else:
        seniority_hint = None

    if isinstance(remote_policy, str):
        remote_policy = remote_policy.strip() or None
    else:
        remote_policy = None

    normalized_stack: list[str] = []
    seen_stack: set[str] = set()
    if isinstance(tech_stack, list):
        for item in tech_stack:
            token = str(item).strip()
            if not token:
                continue

            dedupe_key = token.lower()
            if dedupe_key in seen_stack:
                continue

            normalized_stack.append(token)
            seen_stack.add(dedupe_key)

    try:
        ingestion_relevance_score = (
            max(0.0, min(100.0, float(ingestion_relevance_score)))
            if ingestion_relevance_score is not None
            else None
        )
    except (TypeError, ValueError):
        ingestion_relevance_score = None

    if isinstance(ingestion_relevance_reason, str):
        ingestion_relevance_reason = ingestion_relevance_reason.strip() or None
    else:
        ingestion_relevance_reason = None

    ingestion_exploration_kept = bool(ingestion_exploration_kept)

    return {
        "title": title,
        "company": company,
        "location": location,
        "description": description,
        "requirements": requirements,
        "employment_type": employment_type,
        "seniority_hint": seniority_hint,
        "remote_policy": remote_policy,
        "tech_stack": normalized_stack,
        "ingestion_relevance_score": ingestion_relevance_score,
        "ingestion_relevance_reason": ingestion_relevance_reason,
        "ingestion_exploration_kept": ingestion_exploration_kept,
        "source": source,
        "source_url": source_url,
        "external_job_id": external_job_id,
    }


async def process_normalization_event(message: AbstractIncomingMessage) -> None:
    async with message.process():
        payload = json.loads(message.body.decode())
        event_id = payload.get("event_id")
        run_id = payload.get("run_id")
        user_id = payload.get("user_id")
        raw_job = payload.get("raw_job") or {}
        sequence = int(payload.get("sequence") or 1)
        total_jobs = int(payload.get("total_jobs") or 1)

        if not event_id or not run_id or not user_id:
            logger.error("normalize_worker_invalid_event payload=%s", payload)
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
            logger.info("normalize_worker_duplicate_event event_id=%s", event_id)
            return

        normalized_job = _normalize_raw_job(raw_job)
        stable_dedupe_key = deduplication_policy.dedupe_key(
            normalized_job["source"],
            normalized_job["external_job_id"],
            normalized_job["title"],
            normalized_job["company"],
        )
        external_job_id = normalized_job["external_job_id"] or stable_dedupe_key

        async with postgresql.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO jobs (
                    user_id,
                    title,
                    company,
                    location,
                    description,
                    requirements,
                    employment_type,
                    seniority_hint,
                    remote_policy,
                    tech_stack,
                    ingestion_relevance_score,
                    ingestion_relevance_reason,
                    ingestion_exploration_kept,
                    source,
                    source_url,
                    external_job_id,
                    status
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13, $14, $15, $16, 'NORMALIZED')
                ON CONFLICT (user_id, source, external_job_id)
                WHERE external_job_id IS NOT NULL
                DO UPDATE SET
                    title = EXCLUDED.title,
                    company = EXCLUDED.company,
                    location = EXCLUDED.location,
                    description = EXCLUDED.description,
                    requirements = EXCLUDED.requirements,
                    employment_type = EXCLUDED.employment_type,
                    seniority_hint = EXCLUDED.seniority_hint,
                    remote_policy = EXCLUDED.remote_policy,
                    tech_stack = EXCLUDED.tech_stack,
                    ingestion_relevance_score = EXCLUDED.ingestion_relevance_score,
                    ingestion_relevance_reason = EXCLUDED.ingestion_relevance_reason,
                    ingestion_exploration_kept = EXCLUDED.ingestion_exploration_kept,
                    source_url = EXCLUDED.source_url,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING id
                """,
                user_id,
                normalized_job["title"],
                normalized_job["company"],
                normalized_job["location"],
                normalized_job["description"],
                normalized_job["requirements"],
                normalized_job["employment_type"],
                normalized_job["seniority_hint"],
                normalized_job["remote_policy"],
                json.dumps(normalized_job["tech_stack"]),
                normalized_job["ingestion_relevance_score"],
                normalized_job["ingestion_relevance_reason"],
                normalized_job["ingestion_exploration_kept"],
                normalized_job["source"],
                normalized_job["source_url"],
                external_job_id,
            )

        if not row:
            logger.error("normalize_worker_failed_to_persist run_id=%s user_id=%s", run_id, user_id)
            return

        await messaging_service.publish(
            SCORING_JOBS_QUEUE,
            {
                "event_id": str(uuid.uuid4()),
                "event_version": 1,
                "run_id": run_id,
                "user_id": user_id,
                "job_id": row["id"],
                "sequence": sequence,
                "total_jobs": total_jobs,
                "dedupe_key": stable_dedupe_key,
            },
            rabbitmq.channel,
        )

        logger.info(
            "normalize_worker_processed run_id=%s user_id=%s job_id=%s sequence=%s/%s",
            run_id,
            user_id,
            row["id"],
            sequence,
            total_jobs,
        )


async def run() -> None:
    await postgresql.connect()
    await redis_cache.connect()
    await rabbitmq.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(JOBS_NORMALIZED_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(SCORING_JOBS_QUEUE, durable=True)
    await queue.consume(process_normalization_event)

    try:
        await asyncio.Future()
    finally:
        await rabbitmq.disconnect()
        await redis_cache.disconnect()
        await postgresql.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
