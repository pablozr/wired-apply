from core.config.config import (
    GLOBAL_CATALOG_CLEANUP_LAST_RUN_KEY_PREFIX,
    GLOBAL_CATALOG_CLEANUP_LOCK_KEY_PREFIX,
)
from services.cache import cache_service
from services.common import internal_error


def _cleanup_lock_key() -> str:
    return GLOBAL_CATALOG_CLEANUP_LOCK_KEY_PREFIX


def _cleanup_last_run_key() -> str:
    return GLOBAL_CATALOG_CLEANUP_LAST_RUN_KEY_PREFIX


async def get_global_catalog_cleanup_status(redis_client) -> dict:
    try:
        lock_key = _cleanup_lock_key()
        active_run = await cache_service.get_by_key(lock_key, redis_client)
        active_run_ttl = await cache_service.get_ttl(lock_key, redis_client)
        last_run = await cache_service.get_by_key(
            _cleanup_last_run_key(),
            redis_client,
        )

        active_run_id = active_run if isinstance(active_run, str) else None
        last_run_data = last_run if isinstance(last_run, dict) else None

        return {
            "status": True,
            "message": "Global catalog cleanup status retrieved successfully",
            "data": {
                "isRunning": bool(active_run_id),
                "activeRunId": active_run_id,
                "activeRunTtlSeconds": (
                    active_run_ttl if active_run_ttl > 0 else None
                ),
                "lastRun": last_run_data,
            },
        }
    except Exception as e:
        return internal_error(e)
