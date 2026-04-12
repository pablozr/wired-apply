import json

from core.logger.logger import logger


def _serialize_value(value) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, default=str)
    return str(value)


async def get_by_key(key: str, redis_client) -> dict | str | bool:
    raw = await redis_client.get(key)

    if not raw:
        return False

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


async def set_by_key(key: str, ttl_seconds: int, value: dict, redis_client) -> None:
    await redis_client.setex(key, ttl_seconds, json.dumps(value, default=str))


async def acquire_lock(
    key: str,
    lock_value: str,
    ttl_seconds: int,
    redis_client,
    fail_open: bool = False,
) -> bool:
    try:
        acquired = await redis_client.set(
            key,
            _serialize_value(lock_value),
            ex=ttl_seconds,
            nx=True,
        )
    except Exception as e:
        logger.exception(e)
        if fail_open:
            logger.warning(
                "cache_acquire_lock_fail_open key=%s ttl_seconds=%s",
                key,
                ttl_seconds,
            )
            return True
        raise

    return bool(acquired)


async def release_lock(key: str, lock_value: str, redis_client) -> bool:
    script = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    else
        return 0
    end
    """
    released = await redis_client.eval(script, 1, key, _serialize_value(lock_value))

    return bool(released)


async def get_ttl(key: str, redis_client) -> int:
    return int(await redis_client.ttl(key))


async def delete_by_key(key: str, redis_client) -> None:
    await redis_client.delete(key)
