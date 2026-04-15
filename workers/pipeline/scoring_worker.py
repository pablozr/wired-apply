import asyncio
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import (
    DIGEST_EMAIL_QUEUE,
    PIPELINE_EVENT_DEDUPE_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
    PIPELINE_SCORING_FAILED_KEY_PREFIX,
    PIPELINE_LAST_RUN_TTL_SECONDS,
    PIPELINE_SCORING_DIGEST_TRIGGER_KEY_PREFIX,
    PIPELINE_SCORING_PROGRESS_KEY_PREFIX,
    SCORING_JOBS_QUEUE,
    SHORTLIST_APPLY_QUEUE,
)
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from services.cache import cache_service
from services.messaging import messaging_service
from services.rules import pipeline_state_machine, scoring_policy

DEFAULT_SCORE_WEIGHTS = {
    "role_weight": 0.35,
    "salary_weight": 0.25,
    "location_weight": 0.20,
    "seniority_weight": 0.20,
}


def _event_dedupe_key(event_id: str) -> str:
    return f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


def _scoring_progress_key(run_id: str) -> str:
    return f"{PIPELINE_SCORING_PROGRESS_KEY_PREFIX}:{run_id}"


def _scoring_failed_key(run_id: str) -> str:
    return f"{PIPELINE_SCORING_FAILED_KEY_PREFIX}:{run_id}"


def _scoring_digest_trigger_key(run_id: str) -> str:
    return f"{PIPELINE_SCORING_DIGEST_TRIGGER_KEY_PREFIX}:{run_id}"


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
        progress_key = _scoring_progress_key(run_id)
        failed_key = _scoring_failed_key(run_id)
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

        trigger_key = _scoring_digest_trigger_key(run_id)
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


def _signal_from_job(title: str, location: str | None) -> dict[str, float]:
    normalized_title = title.lower()
    normalized_location = (location or "").lower()

    role_signal = 1.0 if any(k in normalized_title for k in ("engineer", "developer")) else 0.6
    salary_signal = 0.7
    location_signal = 1.0 if "remote" in normalized_location else 0.65

    if "senior" in normalized_title:
        seniority_signal = 0.9
    elif "junior" in normalized_title:
        seniority_signal = 0.55
    else:
        seniority_signal = 0.75

    return {
        "role_weight": role_signal,
        "salary_weight": salary_signal,
        "location_weight": location_signal,
        "seniority_weight": seniority_signal,
    }


def _reason_from_signals(signals: dict[str, float]) -> str:
    return (
        "signals="
        f"role:{signals['role_weight']:.2f},"
        f"salary:{signals['salary_weight']:.2f},"
        f"location:{signals['location_weight']:.2f},"
        f"seniority:{signals['seniority_weight']:.2f}"
    )


def _compute_score(weights: dict[str, float], signals: dict[str, float]) -> float:
    score = 0.0
    for key, weight in weights.items():
        score += max(0.0, float(weight)) * max(0.0, float(signals.get(key, 0.0))) * 100
    return scoring_policy.clamp_score(score)


async def _get_weights(conn, user_id: int) -> dict[str, float]:
    row = await conn.fetchrow(
        """
        SELECT
            role_weight,
            salary_weight,
            location_weight,
            seniority_weight
        FROM score_weights
        WHERE user_id = $1
        """,
        user_id,
    )

    if not row:
        return DEFAULT_SCORE_WEIGHTS

    return {
        "role_weight": float(row["role_weight"]),
        "salary_weight": float(row["salary_weight"]),
        "location_weight": float(row["location_weight"]),
        "seniority_weight": float(row["seniority_weight"]),
    }


async def process_scoring_event(message: AbstractIncomingMessage) -> None:
    async with message.process():
        payload = json.loads(message.body.decode())
        event_id = payload.get("event_id")
        run_id = payload.get("run_id")
        user_id = payload.get("user_id")
        job_id = payload.get("job_id")
        sequence = int(payload.get("sequence") or 1)
        total_jobs = int(payload.get("total_jobs") or 1)

        if not event_id or not run_id or not user_id or not job_id:
            logger.error("scoring_worker_invalid_event payload=%s", payload)
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
            logger.info("scoring_worker_duplicate_event event_id=%s", event_id)
            return

        async with postgresql.pool.acquire() as conn:
            job_row = await conn.fetchrow(
                """
                SELECT id, user_id, title, location, status
                FROM jobs
                WHERE id = $1 AND user_id = $2
                """,
                job_id,
                user_id,
            )

            if not job_row:
                logger.error(
                    "scoring_worker_job_not_found run_id=%s user_id=%s job_id=%s",
                    run_id,
                    user_id,
                    job_id,
                )
                return

            weights = await _get_weights(conn, int(user_id))
            signals = _signal_from_job(job_row["title"], job_row["location"])
            score = _compute_score(weights, signals)
            bucket = scoring_policy.bucket_from_score(score)
            reason = _reason_from_signals(signals)

            await conn.execute(
                """
                INSERT INTO job_scores (
                    user_id,
                    job_id,
                    score,
                    bucket,
                    reason
                )
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id, job_id)
                DO UPDATE SET
                    score = EXCLUDED.score,
                    bucket = EXCLUDED.bucket,
                    reason = EXCLUDED.reason,
                    updated_at = NOW()
                """,
                user_id,
                job_id,
                round(score, 2),
                bucket,
                reason,
            )

            current_status = (job_row["status"] or "INGESTED").strip().upper()
            status_after_scoring = current_status
            if pipeline_state_machine.can_transition(current_status, "SCORED"):
                status_after_scoring = "SCORED"
                await conn.execute(
                    "UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2 AND user_id = $3",
                    status_after_scoring,
                    job_id,
                    user_id,
                )

            if bucket == "A" and pipeline_state_machine.can_transition(
                status_after_scoring,
                "APPLY_READY",
            ):
                await conn.execute(
                    "UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2 AND user_id = $3",
                    "APPLY_READY",
                    job_id,
                    user_id,
                )

        if bucket == "A":
            await messaging_service.publish(
                SHORTLIST_APPLY_QUEUE,
                {
                    "event_id": str(uuid.uuid4()),
                    "event_version": 1,
                    "run_id": run_id,
                    "user_id": user_id,
                    "job_id": job_id,
                    "score": round(score, 2),
                    "bucket": bucket,
                    "sequence": sequence,
                    "total_jobs": total_jobs,
                    "retry_count": 0,
                },
                rabbitmq.channel,
            )

        if await _should_publish_digest(
            str(run_id),
            int(user_id),
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
                    "user_id": user_id,
                },
                rabbitmq.channel,
            )

        logger.info(
            "scoring_worker_processed run_id=%s user_id=%s job_id=%s score=%.2f bucket=%s",
            run_id,
            user_id,
            job_id,
            score,
            bucket,
        )


async def run() -> None:
    await postgresql.connect()
    await redis_cache.connect()
    await rabbitmq.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(SCORING_JOBS_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(SHORTLIST_APPLY_QUEUE, durable=True)
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
