from contextlib import asynccontextmanager

from core.http.http_client import http_client
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache


@asynccontextmanager
async def managed_worker_resources(
    *,
    use_postgresql: bool = False,
    use_redis: bool = False,
    use_rabbitmq: bool = False,
    use_http_client: bool = False,
):
    if use_postgresql:
        await postgresql.connect()

    if use_redis:
        await redis_cache.connect()

    if use_rabbitmq:
        await rabbitmq.connect()

    if use_http_client:
        await http_client.connect()

    try:
        yield
    finally:
        if use_http_client:
            await http_client.disconnect()

        if use_rabbitmq:
            await rabbitmq.disconnect()

        if use_redis:
            await redis_cache.disconnect()

        if use_postgresql:
            await postgresql.disconnect()
