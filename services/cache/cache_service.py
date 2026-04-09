import json


async def get_by_key(key: str, redis_client) -> dict | bool:
    raw = await redis_client.get(key)

    return json.loads(raw) if raw else False


async def set_by_key(key: str, ttl_seconds: int, value: dict, redis_client) -> None:
    await redis_client.setex(key, ttl_seconds, json.dumps(value, default=str))


async def delete_by_key(key: str, redis_client) -> None:
    await redis_client.delete(key)
