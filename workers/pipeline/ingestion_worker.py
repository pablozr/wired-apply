import asyncio
import json
import random
import uuid

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import (
    INGESTION_JOBS_QUEUE,
    INGESTION_RELEVANCE_ENABLED,
    INGESTION_RELEVANCE_EXPLORATION_RATE,
    INGESTION_RELEVANCE_MIN_JOBS,
    INGESTION_RELEVANCE_THRESHOLD,
    JOBS_NORMALIZED_QUEUE,
    PIPELINE_EVENT_DEDUPE_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
)
from core.http.http_client import http_client
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from core.utils.json_utils import ensure_dict, ensure_str_list
from services.cache import cache_service
from services.integrations import ats_service
from services.messaging import messaging_service
from services.rules import ingestion_relevance_policy


async def _get_candidate_context(user_id: int) -> dict:
    async with postgresql.pool.acquire() as conn:
        profile_row = await conn.fetchrow(
            """
            SELECT
                objective,
                seniority,
                target_roles,
                preferred_locations,
                preferred_work_model,
                must_have_skills,
                nice_to_have_skills
            FROM user_profiles
            WHERE user_id = $1
            """,
            user_id,
        )

        resume_row = await conn.fetchrow(
            """
            SELECT
                extracted_json,
                parse_status
            FROM user_resumes
            WHERE user_id = $1 AND is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user_id,
        )

    candidate_context = {
        "objective": None,
        "seniority": None,
        "targetRoles": [],
        "preferredLocations": [],
        "preferredWorkModel": None,
        "mustHaveSkills": [],
        "niceToHaveSkills": [],
        "resumeSkills": [],
        "resumeSeniority": None,
        "resumeParseStatus": None,
    }

    if profile_row:
        candidate_context.update(
            {
                "objective": profile_row["objective"],
                "seniority": profile_row["seniority"],
                "targetRoles": ensure_str_list(profile_row["target_roles"]),
                "preferredLocations": ensure_str_list(profile_row["preferred_locations"]),
                "preferredWorkModel": profile_row["preferred_work_model"],
                "mustHaveSkills": ensure_str_list(profile_row["must_have_skills"]),
                "niceToHaveSkills": ensure_str_list(profile_row["nice_to_have_skills"]),
            }
        )

    if resume_row:
        extracted_json = ensure_dict(resume_row["extracted_json"])

        candidate_context.update(
            {
                "resumeSkills": ensure_str_list(extracted_json.get("skills")),
                "resumeSeniority": extracted_json.get("seniority"),
                "resumeParseStatus": resume_row["parse_status"],
            }
        )

    return candidate_context


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

        dedupe_key = f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"
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

        candidate_context = await _get_candidate_context(int(user_id))
        candidate_has_signals = ingestion_relevance_policy.has_candidate_signals(
            candidate_context
        )

        filtered_jobs: list[dict] = []
        rejected_jobs: list[tuple[float, dict, dict]] = []
        filter_enabled = bool(INGESTION_RELEVANCE_ENABLED)

        for raw_job in raw_jobs:
            relevance = ingestion_relevance_policy.evaluate_job_relevance(
                raw_job,
                candidate_context,
                INGESTION_RELEVANCE_THRESHOLD,
                INGESTION_RELEVANCE_EXPLORATION_RATE,
                random.random(),
            )

            enriched_job = dict(raw_job)
            enriched_job["ingestion_relevance_score"] = relevance["score"]
            enriched_job["ingestion_relevance_reason"] = relevance["reason"]
            enriched_job["ingestion_exploration_kept"] = relevance["explorationKept"]

            if not filter_enabled or not candidate_has_signals:
                enriched_job["ingestion_relevance_reason"] = (
                    "filter_bypassed_missing_candidate_signals"
                )
                filtered_jobs.append(enriched_job)
                continue

            if relevance["keep"]:
                filtered_jobs.append(enriched_job)
            else:
                rejected_jobs.append((relevance["scoreRatio"], enriched_job, relevance))

        minimum_jobs = max(1, int(INGESTION_RELEVANCE_MIN_JOBS))
        if filter_enabled and candidate_has_signals and len(filtered_jobs) < minimum_jobs:
            rejected_jobs.sort(key=lambda item: item[0], reverse=True)
            needed = minimum_jobs - len(filtered_jobs)
            for _, rejected_job, rejected_meta in rejected_jobs[:needed]:
                rejected_job["ingestion_exploration_kept"] = True
                rejected_job["ingestion_relevance_reason"] = (
                    f"{rejected_meta['reason']}|fallback_min_jobs"
                )
                filtered_jobs.append(rejected_job)

        raw_jobs = filtered_jobs

        if not raw_jobs:
            logger.warning(
                "ingestion_worker_all_jobs_filtered run_id=%s user_id=%s fetched=%s",
                run_id,
                user_id,
                len(jobs),
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
            "ingestion_worker_queued_normalization run_id=%s user_id=%s total_jobs=%s fetched_jobs=%s filtered_out=%s filter_enabled=%s candidate_signals=%s sources=%s fallback=%s",
            run_id,
            user_id,
            total_jobs,
            len(jobs),
            max(0, len(jobs) - total_jobs),
            filter_enabled,
            candidate_has_signals,
            source_count,
            fallback_used,
        )


async def run() -> None:
    await postgresql.connect()
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
        await postgresql.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
