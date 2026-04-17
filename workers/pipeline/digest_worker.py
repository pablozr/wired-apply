import asyncio
import json
from datetime import date, datetime, timezone

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import (
    DIGEST_EMAIL_QUEUE,
    EMAIL_QUEUE,
    PIPELINE_EVENT_DEDUPE_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
    PIPELINE_LAST_RUN_KEY_PREFIX,
    PIPELINE_LAST_RUN_TTL_SECONDS,
    PIPELINE_RUN_LOCK_KEY_PREFIX,
    PIPELINE_RUN_AI_CALLS_KEY_PREFIX,
    PIPELINE_RUN_AI_CACHE_HITS_KEY_PREFIX,
    PIPELINE_RUN_AI_CACHE_MISSES_KEY_PREFIX,
    PIPELINE_RUN_AI_PREFILTER_REASON_KEY_PREFIX,
    PIPELINE_RUN_AI_PREFILTER_REJECTED_KEY_PREFIX,
    PIPELINE_RUN_AI_SKIPPED_KEY_PREFIX,
    PIPELINE_SCORING_PROGRESS_KEY_PREFIX,
    PIPELINE_SCORING_FAILED_KEY_PREFIX,
)
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from schemas.digest import digest_from_row
from services.cache import cache_service
from services.messaging import messaging_service
from services.rules.scoring_context_policy import AI_PREFILTER_REASON_CODES


def _event_dedupe_key(event_id: str) -> str:
    return f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


def _pipeline_lock_key(user_id: int) -> str:
    return f"{PIPELINE_RUN_LOCK_KEY_PREFIX}:{user_id}"


def _pipeline_last_run_key(user_id: int) -> str:
    return f"{PIPELINE_LAST_RUN_KEY_PREFIX}:{user_id}"


def _safe_date_from_payload(raw_value) -> date:
    if isinstance(raw_value, str):
        try:
            return date.fromisoformat(raw_value)
        except ValueError:
            return date.today()
    if isinstance(raw_value, date):
        return raw_value
    return date.today()


async def _build_run_metrics(run_id: str, user_id: int) -> dict:
    redis_client = redis_cache.redis
    if redis_client is None:
        return {}

    base_metric_keys = (
        f"{PIPELINE_SCORING_PROGRESS_KEY_PREFIX}:{run_id}",
        f"{PIPELINE_SCORING_FAILED_KEY_PREFIX}:{run_id}",
        f"{PIPELINE_RUN_AI_CALLS_KEY_PREFIX}:{run_id}:{user_id}",
        f"{PIPELINE_RUN_AI_CACHE_HITS_KEY_PREFIX}:{run_id}:{user_id}",
        f"{PIPELINE_RUN_AI_CACHE_MISSES_KEY_PREFIX}:{run_id}:{user_id}",
        f"{PIPELINE_RUN_AI_SKIPPED_KEY_PREFIX}:{run_id}:{user_id}",
        f"{PIPELINE_RUN_AI_PREFILTER_REJECTED_KEY_PREFIX}:{run_id}:{user_id}",
    )
    prefilter_reason_keys = tuple(
        f"{PIPELINE_RUN_AI_PREFILTER_REASON_KEY_PREFIX}:{reason}:{run_id}:{user_id}"
        for reason in AI_PREFILTER_REASON_CODES
    )
    metric_keys = base_metric_keys + prefilter_reason_keys

    try:
        values = await redis_client.mget(*metric_keys)
    except Exception as e:
        logger.exception(e)
        return {}

    def _safe_int(raw_value) -> int:
        try:
            return int(raw_value or 0)
        except (TypeError, ValueError):
            return 0

    processed_count = _safe_int(values[0])
    failed_count = _safe_int(values[1])
    ai_calls = _safe_int(values[2])
    ai_cache_hits = _safe_int(values[3])
    ai_cache_misses = _safe_int(values[4])
    ai_skipped = _safe_int(values[5])
    ai_prefilter_rejected = _safe_int(values[6])

    reason_values = values[7:]
    ai_prefilter_reasons = {
        reason: _safe_int(reason_value)
        for reason, reason_value in zip(AI_PREFILTER_REASON_CODES, reason_values)
        if _safe_int(reason_value) > 0
    }

    ai_cache_checks = ai_cache_hits + ai_cache_misses

    return {
        "jobsProcessed": processed_count,
        "jobsFailed": failed_count,
        "jobsFinished": processed_count + failed_count,
        "aiCalls": ai_calls,
        "aiCacheHits": ai_cache_hits,
        "aiCacheMisses": ai_cache_misses,
        "aiCacheHitRate": (
            round(ai_cache_hits / ai_cache_checks, 4) if ai_cache_checks > 0 else None
        ),
        "aiSkipped": ai_skipped,
        "aiPrefilterRejected": ai_prefilter_rejected,
        "aiPrefilterReasons": ai_prefilter_reasons,
    }


async def _build_or_update_digest(conn, user_id: int, digest_date: date):
    total_jobs = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM jobs
        WHERE user_id = $1 AND DATE(created_at) = $2
        """,
        user_id,
        digest_date,
    )
    total_applications = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM applications
        WHERE user_id = $1 AND DATE(created_at) = $2
        """,
        user_id,
        digest_date,
    )
    total_interviews = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM applications
        WHERE user_id = $1
            AND UPPER(status) LIKE 'INTERVIEW%'
            AND DATE(updated_at) = $2
        """,
        user_id,
        digest_date,
    )

    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "counts": {
            "jobs": int(total_jobs or 0),
            "applications": int(total_applications or 0),
            "interviews": int(total_interviews or 0),
        },
    }

    return await conn.fetchrow(
        """
        INSERT INTO daily_digest (
            user_id,
            digest_date,
            total_jobs,
            total_applications,
            total_interviews,
            payload
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (user_id, digest_date)
        DO UPDATE
        SET
            total_jobs = EXCLUDED.total_jobs,
            total_applications = EXCLUDED.total_applications,
            total_interviews = EXCLUDED.total_interviews,
            payload = EXCLUDED.payload,
            updated_at = NOW()
        RETURNING
            id,
            user_id,
            digest_date,
            total_jobs,
            total_applications,
            total_interviews,
            payload,
            created_at,
            updated_at
        """,
        user_id,
        digest_date,
        int(total_jobs or 0),
        int(total_applications or 0),
        int(total_interviews or 0),
        json.dumps(payload),
    )


def _build_digest_email(fullname: str, digest: dict) -> tuple[str, str, str]:
    subject = f"WiredApply Daily Digest - {digest['digestDate']}"
    message = (
        f"Ola {fullname},\n\n"
        f"Resumo do dia {digest['digestDate']}:\n"
        f"- Jobs ingeridos: {digest['totalJobs']}\n"
        f"- Applications: {digest['totalApplications']}\n"
        f"- Interviews: {digest['totalInterviews']}\n"
    )
    html = (
        "<html><body>"
        f"<h2>Ola, {fullname}</h2>"
        f"<p>Resumo do dia <strong>{digest['digestDate']}</strong>:</p>"
        f"<ul><li>Jobs ingeridos: <strong>{digest['totalJobs']}</strong></li>"
        f"<li>Applications: <strong>{digest['totalApplications']}</strong></li>"
        f"<li>Interviews: <strong>{digest['totalInterviews']}</strong></li></ul>"
        "</body></html>"
    )
    return subject, message + "\nBoa busca!", html


async def process_digest_event(message: AbstractIncomingMessage) -> None:
    async with message.process():
        payload = json.loads(message.body.decode())
        event_id = payload.get("event_id")
        user_id = payload.get("user_id")
        run_id = payload.get("run_id")

        if not event_id or not user_id:
            logger.error("digest_worker_invalid_event payload=%s", payload)
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
            logger.info("digest_worker_duplicate_event event_id=%s", event_id)
            return

        digest_row = None
        digest_date = _safe_date_from_payload(payload.get("digest_date"))

        async with postgresql.pool.acquire() as conn:
            digest_id = payload.get("digest_id")
            if digest_id:
                digest_row = await conn.fetchrow(
                    """
                    SELECT
                        id,
                        user_id,
                        digest_date,
                        total_jobs,
                        total_applications,
                        total_interviews,
                        payload,
                        created_at,
                        updated_at
                    FROM daily_digest
                    WHERE id = $1 AND user_id = $2
                    """,
                    int(digest_id),
                    int(user_id),
                )

            if not digest_row:
                digest_row = await _build_or_update_digest(conn, int(user_id), digest_date)

            user_row = await conn.fetchrow(
                "SELECT fullname, email FROM users WHERE id = $1",
                int(user_id),
            )

        if not digest_row or not user_row:
            logger.error(
                "digest_worker_missing_data user_id=%s run_id=%s digest_date=%s",
                user_id,
                run_id,
                str(digest_date),
            )
            return

        digest_payload = digest_from_row(digest_row)
        subject, message_text, html = _build_digest_email(
            user_row["fullname"],
            {**digest_payload},
        )

        await messaging_service.publish(
            EMAIL_QUEUE,
            {
                "to": user_row["email"],
                "subject": subject,
                "message": message_text,
                "html": html,
            },
            rabbitmq.channel,
        )

        if run_id:
            lock_key = _pipeline_lock_key(int(user_id))
            await cache_service.release_lock(lock_key, str(run_id), redis_cache.redis)

            last_run_key = _pipeline_last_run_key(int(user_id))
            last_run = await cache_service.get_by_key(last_run_key, redis_cache.redis)
            last_run_data = last_run if isinstance(last_run, dict) else {}
            run_metrics = await _build_run_metrics(str(run_id), int(user_id))

            last_run_data.update(
                {
                    "runId": str(run_id),
                    "status": "COMPLETED",
                    "completedAt": datetime.now(timezone.utc).isoformat(),
                    "metrics": run_metrics,
                }
            )
            await cache_service.set_by_key(
                last_run_key,
                PIPELINE_LAST_RUN_TTL_SECONDS,
                last_run_data,
                redis_cache.redis,
            )

        logger.info(
            "digest_worker_processed user_id=%s run_id=%s digest_id=%s",
            user_id,
            run_id,
            digest_payload["digestId"],
        )


async def run() -> None:
    await postgresql.connect()
    await redis_cache.connect()
    await rabbitmq.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(DIGEST_EMAIL_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(EMAIL_QUEUE, durable=True)
    await queue.consume(process_digest_event)

    try:
        await asyncio.Future()
    finally:
        await rabbitmq.disconnect()
        await redis_cache.disconnect()
        await postgresql.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
