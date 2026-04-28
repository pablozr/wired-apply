from core.config.config import (
    PIPELINE_LAST_RUN_TTL_SECONDS,
    PIPELINE_RUN_AI_CALLS_KEY_PREFIX,
    PIPELINE_RUN_AI_CACHE_HITS_KEY_PREFIX,
    PIPELINE_RUN_AI_CACHE_MISSES_KEY_PREFIX,
    PIPELINE_RUN_AI_PREFILTER_REASON_KEY_PREFIX,
    PIPELINE_RUN_AI_PREFILTER_REJECTED_KEY_PREFIX,
    PIPELINE_RUN_AI_SKIPPED_KEY_PREFIX,
    PIPELINE_RUN_METRICS_KEY_PREFIX,
    PIPELINE_SCORING_FAILED_KEY_PREFIX,
    PIPELINE_SCORING_PROGRESS_KEY_PREFIX,
)
from core.logger.logger import logger
from services.rules.scoring_context_policy import AI_PREFILTER_REASON_CODES

RUN_METRIC_FIELD_JOBS_PROCESSED = "jobs_processed"
RUN_METRIC_FIELD_JOBS_FAILED = "jobs_failed"
RUN_METRIC_FIELD_DETERMINISTIC_PROCESSED = "deterministic_processed"
RUN_METRIC_FIELD_AI_PLANNED = "ai_planned"
RUN_METRIC_FIELD_INGESTION_FETCHED = "ingestion_fetched"
RUN_METRIC_FIELD_INGESTION_KEPT = "ingestion_kept"
RUN_METRIC_FIELD_INGESTION_FILTERED = "ingestion_filtered"
RUN_METRIC_FIELD_INGESTION_HARD_REJECTED = "ingestion_hard_rejected"
RUN_METRIC_FIELD_NORMALIZED = "normalized"
RUN_METRIC_FIELD_AI_CALLS = "ai_calls"
RUN_METRIC_FIELD_AI_CACHE_HITS = "ai_cache_hits"
RUN_METRIC_FIELD_AI_CACHE_MISSES = "ai_cache_misses"
RUN_METRIC_FIELD_AI_SKIPPED = "ai_skipped"
RUN_METRIC_FIELD_AI_PREFILTER_REJECTED = "ai_prefilter_rejected"
RUN_METRIC_FIELD_AI_PREFILTER_REASON_PREFIX = "ai_prefilter_reason"


def build_run_metrics_key(run_id: str, user_id: int) -> str:
    return f"{PIPELINE_RUN_METRICS_KEY_PREFIX}:{run_id}:{int(user_id)}"


def build_prefilter_reason_field(reason: str) -> str:
    reason_value = str(reason or "").strip().lower()
    return f"{RUN_METRIC_FIELD_AI_PREFILTER_REASON_PREFIX}:{reason_value}"


def _safe_int(raw_value) -> int:
    try:
        return int(raw_value or 0)
    except (TypeError, ValueError):
        return 0


def _format_metrics_payload(
    processed_raw,
    failed_raw,
    deterministic_processed_raw,
    ai_planned_raw,
    ingestion_fetched_raw,
    ingestion_kept_raw,
    ingestion_filtered_raw,
    ingestion_hard_rejected_raw,
    normalized_raw,
    ai_calls_raw,
    ai_cache_hits_raw,
    ai_cache_misses_raw,
    ai_skipped_raw,
    ai_prefilter_rejected_raw,
    reason_values,
) -> dict:
    processed_count = _safe_int(processed_raw)
    failed_count = _safe_int(failed_raw)
    deterministic_processed = _safe_int(deterministic_processed_raw)
    ai_planned = _safe_int(ai_planned_raw)
    ingestion_fetched = _safe_int(ingestion_fetched_raw)
    ingestion_kept = _safe_int(ingestion_kept_raw)
    ingestion_filtered = _safe_int(ingestion_filtered_raw)
    ingestion_hard_rejected = _safe_int(ingestion_hard_rejected_raw)
    normalized_count = _safe_int(normalized_raw)
    ai_calls = _safe_int(ai_calls_raw)
    ai_cache_hits = _safe_int(ai_cache_hits_raw)
    ai_cache_misses = _safe_int(ai_cache_misses_raw)
    ai_skipped = _safe_int(ai_skipped_raw)
    ai_prefilter_rejected = _safe_int(ai_prefilter_rejected_raw)

    ai_prefilter_reasons = {
        reason: count
        for reason, count in (
            (reason, _safe_int(reason_value))
            for reason, reason_value in zip(AI_PREFILTER_REASON_CODES, reason_values)
        )
        if count > 0
    }

    ai_cache_checks = ai_cache_hits + ai_cache_misses

    return {
        "jobsProcessed": processed_count,
        "jobsFailed": failed_count,
        "jobsFinished": processed_count + failed_count,
        "deterministicProcessed": deterministic_processed,
        "aiPlanned": ai_planned,
        "ingestionFetched": ingestion_fetched,
        "ingestionKept": ingestion_kept,
        "ingestionFiltered": ingestion_filtered,
        "ingestionHardRejected": ingestion_hard_rejected,
        "normalized": normalized_count,
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


async def increment_pipeline_run_metric(
    run_id: str,
    user_id: int,
    field_name: str,
    redis_client,
    delta: int = 1,
) -> int:
    if redis_client is None:
        return 0

    metric_key = build_run_metrics_key(run_id, user_id)

    try:
        metric_count = int(await redis_client.hincrby(metric_key, field_name, int(delta)))
        await redis_client.expire(metric_key, PIPELINE_LAST_RUN_TTL_SECONDS)
        return metric_count
    except Exception as e:
        logger.exception(e)
        return 0


async def get_scoring_progress_counts(run_id: str, user_id: int, redis_client) -> tuple[int, int]:
    if redis_client is None:
        return 0, 0

    metric_key = build_run_metrics_key(run_id, user_id)

    try:
        metrics_key_exists = bool(await redis_client.exists(metric_key))
        if metrics_key_exists:
            processed_raw, failed_raw = await redis_client.hmget(
                metric_key,
                RUN_METRIC_FIELD_JOBS_PROCESSED,
                RUN_METRIC_FIELD_JOBS_FAILED,
            )
            return _safe_int(processed_raw), _safe_int(failed_raw)
    except Exception as e:
        logger.exception(e)

    try:
        processed_raw, failed_raw = await redis_client.mget(
            f"{PIPELINE_SCORING_PROGRESS_KEY_PREFIX}:{run_id}",
            f"{PIPELINE_SCORING_FAILED_KEY_PREFIX}:{run_id}",
        )
        return _safe_int(processed_raw), _safe_int(failed_raw)
    except Exception as e:
        logger.exception(e)
        return 0, 0


async def build_pipeline_run_metrics(run_id: str, user_id: int, redis_client) -> dict:
    if redis_client is None:
        return {}

    metric_key = build_run_metrics_key(run_id, user_id)
    reason_fields = tuple(build_prefilter_reason_field(reason) for reason in AI_PREFILTER_REASON_CODES)

    try:
        metrics_key_exists = bool(await redis_client.exists(metric_key))
        if metrics_key_exists:
            values = await redis_client.hmget(
                metric_key,
                RUN_METRIC_FIELD_JOBS_PROCESSED,
                RUN_METRIC_FIELD_JOBS_FAILED,
                RUN_METRIC_FIELD_DETERMINISTIC_PROCESSED,
                RUN_METRIC_FIELD_AI_PLANNED,
                RUN_METRIC_FIELD_INGESTION_FETCHED,
                RUN_METRIC_FIELD_INGESTION_KEPT,
                RUN_METRIC_FIELD_INGESTION_FILTERED,
                RUN_METRIC_FIELD_INGESTION_HARD_REJECTED,
                RUN_METRIC_FIELD_NORMALIZED,
                RUN_METRIC_FIELD_AI_CALLS,
                RUN_METRIC_FIELD_AI_CACHE_HITS,
                RUN_METRIC_FIELD_AI_CACHE_MISSES,
                RUN_METRIC_FIELD_AI_SKIPPED,
                RUN_METRIC_FIELD_AI_PREFILTER_REJECTED,
                *reason_fields,
            )
            return _format_metrics_payload(
                values[0],
                values[1],
                values[2],
                values[3],
                values[4],
                values[5],
                values[6],
                values[7],
                values[8],
                values[9],
                values[10],
                values[11],
                values[12],
                values[13],
                values[14:],
            )
    except Exception as e:
        logger.exception(e)

    legacy_base_keys = (
        f"{PIPELINE_SCORING_PROGRESS_KEY_PREFIX}:{run_id}",
        f"{PIPELINE_SCORING_FAILED_KEY_PREFIX}:{run_id}",
        f"{PIPELINE_RUN_AI_CALLS_KEY_PREFIX}:{run_id}:{user_id}",
        f"{PIPELINE_RUN_AI_CACHE_HITS_KEY_PREFIX}:{run_id}:{user_id}",
        f"{PIPELINE_RUN_AI_CACHE_MISSES_KEY_PREFIX}:{run_id}:{user_id}",
        f"{PIPELINE_RUN_AI_SKIPPED_KEY_PREFIX}:{run_id}:{user_id}",
        f"{PIPELINE_RUN_AI_PREFILTER_REJECTED_KEY_PREFIX}:{run_id}:{user_id}",
    )
    legacy_reason_keys = tuple(
        f"{PIPELINE_RUN_AI_PREFILTER_REASON_KEY_PREFIX}:{reason}:{run_id}:{user_id}"
        for reason in AI_PREFILTER_REASON_CODES
    )

    try:
        legacy_values = await redis_client.mget(*(legacy_base_keys + legacy_reason_keys))
    except Exception as e:
        logger.exception(e)
        return {}

    return _format_metrics_payload(
        legacy_values[0],
        legacy_values[1],
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        legacy_values[2],
        legacy_values[3],
        legacy_values[4],
        legacy_values[5],
        legacy_values[6],
        legacy_values[7:],
    )
