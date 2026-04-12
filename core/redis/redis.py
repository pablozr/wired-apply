from redis import asyncio as redis_asyncio

from core.config.config import settings


class Redis:
    redis: redis_asyncio.Redis | None = None

    async def connect(self):
        self.redis = redis_asyncio.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            password=settings.REDIS_PASSWORD or None,
            decode_responses=True,
        )

    async def disconnect(self):
        if self.redis is not None:
            await self.redis.aclose()

    async def get_redis(self):
        yield self.redis


redis_cache = Redis()
