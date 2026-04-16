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
from services.ai import ai_service
from services.cache import cache_service
from services.integrations import playwright_service
from services.messaging import messaging_service
from services.rules import application_constraints


def _event_dedupe_key(event_id: str) -> str:
    return f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


def _list_from_value(value) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []

    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    return []


def _dict_from_value(value) -> dict:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return {}

    return value if isinstance(value, dict) else {}


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
                    SELECT
                        id,
                        title,
                        company,
                        location,
                        source,
                        source_url,
                        status
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

                profile_row = await conn.fetchrow(
                    """
                    SELECT
                        objective,
                        seniority,
                        target_roles,
                        preferred_locations,
                        preferred_work_model,
                        salary_expectation,
                        must_have_skills,
                        nice_to_have_skills
                    FROM user_profiles
                    WHERE user_id = $1
                    """,
                    int(user_id),
                )

                resume_row = await conn.fetchrow(
                    """
                    SELECT extracted_json, parse_status, parse_confidence
                    FROM user_resumes
                    WHERE user_id = $1 AND is_active = TRUE
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    int(user_id),
                )

                profile_context = {}
                if profile_row:
                    profile_context = {
                        "objective": profile_row["objective"],
                        "seniority": profile_row["seniority"],
                        "targetRoles": _list_from_value(profile_row["target_roles"]),
                        "preferredLocations": _list_from_value(
                            profile_row["preferred_locations"]
                        ),
                        "preferredWorkModel": profile_row["preferred_work_model"],
                        "salaryExpectation": profile_row["salary_expectation"],
                        "mustHaveSkills": _list_from_value(
                            profile_row["must_have_skills"]
                        ),
                        "niceToHaveSkills": _list_from_value(
                            profile_row["nice_to_have_skills"]
                        ),
                    }

                resume_context = {}
                if resume_row:
                    resume_context = _dict_from_value(resume_row["extracted_json"])
                    resume_context["parseStatus"] = resume_row["parse_status"]
                    parse_confidence = resume_row["parse_confidence"]
                    resume_context["parseConfidence"] = (
                        float(parse_confidence) if parse_confidence is not None else None
                    )

                job_context = {
                    "jobId": int(job_row["id"]),
                    "title": job_row["title"],
                    "company": job_row["company"],
                    "location": job_row["location"],
                    "source": job_row["source"],
                    "sourceUrl": job_row["source_url"],
                }

                ai_payload_response = await ai_service.build_auto_apply_payload(
                    job_context=job_context,
                    profile_context=profile_context,
                    resume_context=resume_context,
                )
                ai_payload = ai_payload_response.get("data", {})

                playwright_response = await playwright_service.prepare_assisted_apply(
                    run_id=str(run_id),
                    user_id=int(user_id),
                    job_id=int(job_id),
                    job_context=job_context,
                    auto_apply_payload=ai_payload,
                )

                notes_parts = [
                    "Auto-shortlisted by worker; waiting human confirmation for submit."
                ]

                ai_confidence = ai_payload.get("confidence")
                try:
                    ai_confidence_value = (
                        float(ai_confidence) if ai_confidence is not None else None
                    )
                except (TypeError, ValueError):
                    ai_confidence_value = None

                if ai_payload_response.get("status"):
                    if ai_confidence_value is None:
                        notes_parts.append("AI form payload generated.")
                    else:
                        notes_parts.append(
                            f"AI form payload generated (confidence={ai_confidence_value:.2f})."
                        )
                else:
                    notes_parts.append("AI form payload failed. Using fallback responses.")

                if playwright_response.get("status"):
                    notes_parts.append("Playwright assisted mode prepared.")
                else:
                    notes_parts.append(
                        "Playwright assisted mode unavailable; queued for manual review."
                    )

                answers_preview = ai_payload.get("answers")
                if isinstance(answers_preview, dict) and answers_preview:
                    serialized_answers = json.dumps(answers_preview)
                    notes_parts.append(
                        f"answersPreview={serialized_answers[:450]}"
                    )

                notes = " ".join(notes_parts)

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
                    notes,
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
