from core.config.config import (
    PIPELINE_RUN_AI_CALLS_KEY_PREFIX,
    PIPELINE_RUN_AI_CACHE_HITS_KEY_PREFIX,
    PIPELINE_RUN_AI_CACHE_MISSES_KEY_PREFIX,
    PIPELINE_RUN_AI_PREFILTER_REASON_KEY_PREFIX,
    PIPELINE_RUN_AI_PREFILTER_REJECTED_KEY_PREFIX,
    PIPELINE_RUN_AI_SKIPPED_KEY_PREFIX,
    PIPELINE_SCORING_FAILED_KEY_PREFIX,
    PIPELINE_SCORING_PROGRESS_KEY_PREFIX,
)
from core.logger.logger import logger
from services.rules.scoring_context_policy import AI_PREFILTER_REASON_CODES


async def build_pipeline_run_metrics(run_id: str, user_id: int, redis_client) -> dict:
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