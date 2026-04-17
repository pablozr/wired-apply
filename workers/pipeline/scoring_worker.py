import asyncio
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from prompts.ai_prompts import SCORING_PROMPT
from core.config.config import (
    AI_MAX_CALLS_PER_RUN,
    AI_TYR_MODEL,
    AI_MIN_CONTEXT_QUALITY,
    AI_SCORING_CACHE_ENABLED,
    AI_SCORING_ENABLED,
    AI_SCORING_MAX_SENIORITY_GAP,
    AI_SCORING_MIN_DETERMINISTIC_SCORE,
    AI_SCORING_MIN_LOCATION_SIGNAL,
    AI_SCORING_MIN_ROLE_MATCH,
    AI_SCORING_MIN_SKILL_OVERLAP,
    AI_SCORING_MIN_WORK_MODEL_SIGNAL,
    DIGEST_EMAIL_QUEUE,
    PIPELINE_EVENT_DEDUPE_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
    PIPELINE_SCORING_FAILED_KEY_PREFIX,
    PIPELINE_LAST_RUN_TTL_SECONDS,
    PIPELINE_SCORING_DIGEST_TRIGGER_KEY_PREFIX,
    PIPELINE_SCORING_PROGRESS_KEY_PREFIX,
    PIPELINE_RUN_AI_CALLS_KEY_PREFIX,
    PIPELINE_RUN_AI_CACHE_HITS_KEY_PREFIX,
    PIPELINE_RUN_AI_CACHE_MISSES_KEY_PREFIX,
    PIPELINE_RUN_AI_PREFILTER_REASON_KEY_PREFIX,
    PIPELINE_RUN_AI_PREFILTER_REJECTED_KEY_PREFIX,
    PIPELINE_RUN_AI_SKIPPED_KEY_PREFIX,
    SCORING_JOBS_QUEUE,
)
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from core.utils.json_utils import ensure_dict, ensure_str_list
from services.ai import ai_service
from services.cache import cache_service
from services.weights.weights_service import get_score_weights
from services.messaging import messaging_service
from services.rules import pipeline_state_machine, scoring_policy
from services.rules.scoring_context_policy import (
    AI_PREFILTER_REASON_CODES,
    build_ai_cache_versions,
    build_ai_context_hash,
    compose_final_score,
    compute_score,
    context_quality as compute_context_quality,
    evaluate_ai_prefilter,
    reason_from_signals,
    signal_from_context,
)
from services.user.user_context_service import get_ai_context


async def _should_publish_digest(
    run_id: str,
    user_id: int,
    total_jobs: int,
    scoring_succeeded: bool,
) -> bool:
    redis_client = redis_cache.redis
    if redis_client is None:
        logger.error("scoring_worker_redis_not_connected run_id=%s user_id=%s", run_id, user_id)
        return False

    try:
        expected_jobs = max(1, int(total_jobs or 1))
        progress_key = f"{PIPELINE_SCORING_PROGRESS_KEY_PREFIX}:{run_id}"
        failed_key = f"{PIPELINE_SCORING_FAILED_KEY_PREFIX}:{run_id}"
        counter_key = progress_key if scoring_succeeded else failed_key

        updated_count = int(await redis_client.incr(counter_key))
        if updated_count == 1:
            await redis_client.expire(counter_key, PIPELINE_LAST_RUN_TTL_SECONDS)

        processed_raw, failed_raw = await redis_client.mget(progress_key, failed_key)
        processed_count = int(processed_raw or 0)
        failed_count = int(failed_raw or 0)
        finished_count = processed_count + failed_count

        logger.info(
            "scoring_worker_progress run_id=%s user_id=%s processed=%s failed=%s finished=%s expected=%s success=%s",
            run_id,
            user_id,
            processed_count,
            failed_count,
            finished_count,
            expected_jobs,
            scoring_succeeded,
        )

        if finished_count < expected_jobs:
            return False

        trigger_key = f"{PIPELINE_SCORING_DIGEST_TRIGGER_KEY_PREFIX}:{run_id}"
        trigger_value = f"{run_id}:{user_id}"
        return await cache_service.acquire_lock(
            trigger_key,
            trigger_value,
            PIPELINE_LAST_RUN_TTL_SECONDS,
            redis_client,
            fail_open=False,
        )
    except Exception as e:
        logger.exception(e)
        return False


async def _increment_run_metric(key_prefix: str, run_id: str, user_id: int) -> None:
    redis_client = redis_cache.redis
    if redis_client is None:
        return

    try:
        metric_key = f"{key_prefix}:{run_id}:{user_id}"
        metric_count = int(await redis_client.incr(metric_key))
        if metric_count == 1:
            await redis_client.expire(metric_key, PIPELINE_LAST_RUN_TTL_SECONDS)
    except Exception as e:
        logger.exception(e)


async def process_scoring_event(message: AbstractIncomingMessage) -> None:
    async with message.process():
        payload = json.loads(message.body.decode())
        event_id = payload.get("event_id")
        run_id = payload.get("run_id")
        user_id = payload.get("user_id")
        job_id = payload.get("job_id")
        sequence = int(payload.get("sequence") or 1)
        total_jobs = int(payload.get("total_jobs") or 1)
        force_rescore = bool(payload.get("force_rescore", False))
        date_from = payload.get("date_from")
        date_to = payload.get("date_to")
        days_range = payload.get("days_range")

        if not event_id or not run_id or not user_id or not job_id:
            logger.error("scoring_worker_invalid_event payload=%s", payload)
            return

        user_id_int = int(user_id)
        job_id_int = int(job_id)

        dedupe_key = f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"
        is_new_event = await cache_service.acquire_lock(
            dedupe_key,
            str(event_id),
            PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
            redis_cache.redis,
            fail_open=True,
        )

        if not is_new_event:
            logger.info("scoring_worker_duplicate_event event_id=%s", event_id)
            return

        async with postgresql.pool.acquire() as conn:
            job_row = await conn.fetchrow(
                """
                SELECT
                    id,
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
                    source,
                    source_url,
                    status
                FROM jobs
                WHERE id = $1 AND user_id = $2
                """,
                job_id_int,
                user_id_int,
            )

            if not job_row:
                logger.error(
                    "scoring_worker_job_not_found run_id=%s user_id=%s job_id=%s",
                    run_id,
                    user_id_int,
                    job_id_int,
                )
                return

            weights = await get_score_weights(conn, user_id_int)
            ai_score = None
            ai_confidence = None
            ai_reason = None
            ai_breakdown = None
            ai_context_hash = None
            ai_skipped_reason = None
            ai_cache_hit = False
            ai_calls_count = 0
            effective_ai_weight = 0.0
            ai_used = False

            job_context = {
                "jobId": int(job_row["id"]),
                "title": job_row["title"],
                "company": job_row["company"],
                "location": job_row["location"],
                "description": job_row["description"],
                "requirements": job_row["requirements"],
                "employmentType": job_row["employment_type"],
                "seniorityHint": job_row["seniority_hint"],
                "remotePolicy": job_row["remote_policy"],
                "techStack": ensure_str_list(job_row["tech_stack"]),
                "source": job_row["source"],
                "sourceUrl": job_row["source_url"],
            }
            profile_context, resume_context = await get_ai_context(conn, user_id_int)
            signals, signal_details = signal_from_context(
                job_context,
                profile_context,
                resume_context,
            )
            deterministic_score = compute_score(weights, signals)
            context_quality_score = compute_context_quality(
                job_context,
                profile_context,
                resume_context,
            )
            prefilter_result = evaluate_ai_prefilter(
                deterministic_score,
                signal_details,
                AI_SCORING_MIN_DETERMINISTIC_SCORE,
                AI_SCORING_MIN_ROLE_MATCH,
                AI_SCORING_MIN_SKILL_OVERLAP,
                AI_SCORING_MIN_LOCATION_SIGNAL,
                AI_SCORING_MIN_WORK_MODEL_SIGNAL,
                AI_SCORING_MAX_SENIORITY_GAP,
            )
            prefilter_metrics = ensure_dict(prefilter_result.get("metrics"))
            prefilter_reason = str(prefilter_result.get("reason") or "").strip() or None

            role_match = float(prefilter_metrics.get("roleMatch") or 0.0)
            skill_hits = int(prefilter_metrics.get("skillHits") or 0)
            skill_total = int(prefilter_metrics.get("skillTotal") or 0)
            skill_overlap = float(prefilter_metrics.get("skillOverlap") or 0.0)
            location_signal = float(prefilter_metrics.get("locationSignal") or 0.0)
            work_model_signal = float(prefilter_metrics.get("workModelSignal") or 0.0)
            seniority_gap_raw = prefilter_metrics.get("seniorityGap")
            seniority_gap = int(seniority_gap_raw) if seniority_gap_raw is not None else None

            ai_context_hash = build_ai_context_hash(
                job_context,
                profile_context,
                resume_context,
            )
            cache_versions = build_ai_cache_versions(
                job_context,
                profile_context,
                resume_context,
                SCORING_PROMPT,
                AI_TYR_MODEL,
            )
            ai_job_hash = cache_versions["jobHash"]
            ai_profile_version = cache_versions["profileVersion"]
            ai_resume_version = cache_versions["resumeVersion"]
            ai_prompt_version = cache_versions["promptVersion"]
            ai_model_version = cache_versions["modelVersion"]
            ai_cache_key = cache_versions["cacheKey"]

            existing_score_row = None
            if AI_SCORING_ENABLED and AI_SCORING_CACHE_ENABLED:
                existing_score_row = await conn.fetchrow(
                    """
                    SELECT
                        ai_context_hash,
                        ai_cache_key,
                        ai_score,
                        ai_confidence,
                        ai_reason,
                        ai_breakdown
                    FROM job_scores
                    WHERE user_id = $1 AND job_id = $2
                    """,
                    user_id_int,
                    job_id_int,
                )

            cache_signature_matches = False
            if existing_score_row:
                cached_cache_key = str(existing_score_row["ai_cache_key"] or "").strip()
                if cached_cache_key:
                    cache_signature_matches = cached_cache_key == ai_cache_key
                else:
                    cache_signature_matches = (
                        existing_score_row["ai_context_hash"] == ai_context_hash
                    )

            if AI_SCORING_ENABLED:
                if (
                    AI_SCORING_CACHE_ENABLED
                    and not force_rescore
                    and existing_score_row
                    and cache_signature_matches
                    and existing_score_row["ai_score"] is not None
                ):
                    ai_score = scoring_policy.clamp_score(float(existing_score_row["ai_score"]))
                    ai_confidence = (
                        float(existing_score_row["ai_confidence"])
                        if existing_score_row["ai_confidence"] is not None
                        else None
                    )
                    ai_reason = existing_score_row["ai_reason"]
                    ai_breakdown = ensure_dict(existing_score_row["ai_breakdown"])
                    ai_cache_hit = True
                    ai_skipped_reason = "cache_hit"
                elif prefilter_reason:
                    ai_skipped_reason = prefilter_reason
                elif context_quality_score < float(AI_MIN_CONTEXT_QUALITY):
                    ai_skipped_reason = "low_context_quality"
                else:
                    ai_call_allowed = True
                    redis_client = redis_cache.redis
                    if redis_client is not None:
                        ai_calls_key = f"{PIPELINE_RUN_AI_CALLS_KEY_PREFIX}:{run_id}:{user_id_int}"
                        ai_calls_count = int(await redis_client.incr(ai_calls_key))
                        if ai_calls_count == 1:
                            await redis_client.expire(ai_calls_key, PIPELINE_LAST_RUN_TTL_SECONDS)

                        if ai_calls_count > max(1, int(AI_MAX_CALLS_PER_RUN)):
                            ai_call_allowed = False
                            ai_skipped_reason = "run_ai_cap_reached"

                    if ai_call_allowed:
                        ai_response = await ai_service.score_job_fit(
                            job_context=job_context,
                            profile_context=profile_context,
                            resume_context=resume_context,
                        )

                        if ai_response.get("status"):
                            ai_payload = ai_response.get("data", {})

                            try:
                                ai_score = scoring_policy.clamp_score(
                                    float(ai_payload.get("aiScore"))
                                )
                            except (TypeError, ValueError):
                                ai_score = None

                            try:
                                raw_confidence = ai_payload.get("confidence")
                                ai_confidence = (
                                    max(0.0, min(1.0, float(raw_confidence)))
                                    if raw_confidence is not None
                                    else None
                                )
                            except (TypeError, ValueError):
                                ai_confidence = None

                            ai_reason = ai_payload.get("reason")
                            ai_breakdown = ensure_dict(ai_payload.get("breakdown"))
                            ai_skipped_reason = None
                        else:
                            ai_reason = ai_response.get("message")
                            ai_skipped_reason = "ai_scoring_failed"
            else:
                ai_skipped_reason = "ai_disabled"

            if AI_SCORING_ENABLED and AI_SCORING_CACHE_ENABLED and not force_rescore:
                if ai_cache_hit:
                    await _increment_run_metric(
                        PIPELINE_RUN_AI_CACHE_HITS_KEY_PREFIX,
                        str(run_id),
                        user_id_int,
                    )
                else:
                    await _increment_run_metric(
                        PIPELINE_RUN_AI_CACHE_MISSES_KEY_PREFIX,
                        str(run_id),
                        user_id_int,
                    )

            if ai_skipped_reason and ai_skipped_reason != "cache_hit":
                await _increment_run_metric(
                    PIPELINE_RUN_AI_SKIPPED_KEY_PREFIX,
                    str(run_id),
                    user_id_int,
                )

            if ai_skipped_reason in AI_PREFILTER_REASON_CODES:
                await _increment_run_metric(
                    PIPELINE_RUN_AI_PREFILTER_REJECTED_KEY_PREFIX,
                    str(run_id),
                    user_id_int,
                )
                await _increment_run_metric(
                    f"{PIPELINE_RUN_AI_PREFILTER_REASON_KEY_PREFIX}:{ai_skipped_reason}",
                    str(run_id),
                    user_id_int,
                )

            final_score, effective_ai_weight, ai_used = compose_final_score(
                deterministic_score,
                ai_score,
                ai_confidence,
                context_quality_score,
            )
            score = final_score
            bucket = scoring_policy.bucket_from_score(final_score)
            reason = reason_from_signals(signals, signal_details)

            deterministic_score_rounded = round(deterministic_score, 2)
            ai_score_rounded = round(ai_score, 2) if ai_score is not None else None
            ai_confidence_rounded = (
                round(ai_confidence, 2) if ai_confidence is not None else None
            )
            final_score_rounded = round(final_score, 2)
            context_quality_rounded = round(context_quality_score, 2)
            ai_delta = (
                round(abs(ai_score - deterministic_score), 2)
                if ai_score is not None
                else None
            )

            await conn.execute(
                """
                INSERT INTO job_scores (
                    user_id,
                    job_id,
                    score,
                    deterministic_score,
                    ai_score,
                    ai_confidence,
                    final_score,
                    bucket,
                    reason,
                    ai_reason,
                    ai_breakdown,
                    ai_context_hash,
                    ai_job_hash,
                    ai_profile_version,
                    ai_resume_version,
                    ai_prompt_version,
                    ai_model_version,
                    ai_cache_key,
                    ai_skipped_reason
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $14, $15, $16, $17, $18, $19)
                ON CONFLICT (user_id, job_id)
                DO UPDATE SET
                    score = EXCLUDED.score,
                    deterministic_score = EXCLUDED.deterministic_score,
                    ai_score = EXCLUDED.ai_score,
                    ai_confidence = EXCLUDED.ai_confidence,
                    final_score = EXCLUDED.final_score,
                    bucket = EXCLUDED.bucket,
                    reason = EXCLUDED.reason,
                    ai_reason = EXCLUDED.ai_reason,
                    ai_breakdown = EXCLUDED.ai_breakdown,
                    ai_context_hash = EXCLUDED.ai_context_hash,
                    ai_job_hash = EXCLUDED.ai_job_hash,
                    ai_profile_version = EXCLUDED.ai_profile_version,
                    ai_resume_version = EXCLUDED.ai_resume_version,
                    ai_prompt_version = EXCLUDED.ai_prompt_version,
                    ai_model_version = EXCLUDED.ai_model_version,
                    ai_cache_key = EXCLUDED.ai_cache_key,
                    ai_skipped_reason = EXCLUDED.ai_skipped_reason,
                    updated_at = NOW()
                """,
                user_id_int,
                job_id_int,
                final_score_rounded,
                deterministic_score_rounded,
                ai_score_rounded,
                ai_confidence_rounded,
                final_score_rounded,
                bucket,
                reason,
                ai_reason,
                json.dumps(ai_breakdown) if ai_breakdown else None,
                ai_context_hash,
                ai_job_hash,
                ai_profile_version,
                ai_resume_version,
                ai_prompt_version,
                ai_model_version,
                ai_cache_key,
                ai_skipped_reason,
            )

            current_status = (job_row["status"] or "INGESTED").strip().upper()
            if pipeline_state_machine.can_transition(current_status, "SCORED"):
                await conn.execute(
                    "UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2 AND user_id = $3",
                    "SCORED",
                    job_id_int,
                    user_id_int,
                )



        if await _should_publish_digest(
            str(run_id),
            user_id_int,
            total_jobs,
            scoring_succeeded=True,
        ):
            await messaging_service.publish(
                DIGEST_EMAIL_QUEUE,
                {
                    "event_id": str(uuid.uuid4()),
                    "event_version": 1,
                    "source": "pipeline",
                    "run_id": run_id,
                    "user_id": user_id_int,
                },
                rabbitmq.channel,
            )

        logger.info(
            "scoring_worker_processed run_id=%s user_id=%s job_id=%s sequence=%s/%s date_from=%s date_to=%s days_range=%s force_rescore=%s final_score=%.2f deterministic_score=%.2f ai_score=%s ai_confidence=%s context_quality=%.2f role_match=%.2f skill_overlap=%.2f location_signal=%.2f work_model_signal=%.2f seniority_gap=%s prefilter_reason=%s effective_ai_weight=%.4f ai_used=%s ai_cache_hit=%s ai_calls_count=%s ai_skipped_reason=%s ai_delta=%s bucket=%s",
            run_id,
            user_id_int,
            job_id_int,
            sequence,
            total_jobs,
            date_from,
            date_to,
            days_range,
            force_rescore,
            score,
            deterministic_score_rounded,
            ai_score_rounded,
            ai_confidence_rounded,
            context_quality_rounded,
            round(role_match, 2),
            round(skill_overlap, 2),
            round(location_signal, 2),
            round(work_model_signal, 2),
            seniority_gap,
            prefilter_reason,
            effective_ai_weight,
            ai_used,
            ai_cache_hit,
            ai_calls_count,
            ai_skipped_reason,
            ai_delta,
            bucket,
        )


async def run() -> None:
    await postgresql.connect()
    await redis_cache.connect()
    await rabbitmq.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(SCORING_JOBS_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(DIGEST_EMAIL_QUEUE, durable=True)
    await queue.consume(process_scoring_event)

    try:
        await asyncio.Future()
    finally:
        await rabbitmq.disconnect()
        await redis_cache.disconnect()
        await postgresql.disconnect()


if __name__ == "__main__":
    asyncio.run(run())

