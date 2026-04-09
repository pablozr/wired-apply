from typing import Optional

import asyncpg

from core.config.config import settings


class PostgreSQL:
    pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}",
            min_size=1,
            max_size=3,
        )

    async def disconnect(self):
        await self.pool.close()

    async def get_db(self):
        async with self.pool.acquire() as conn:
            yield conn


postgresql = PostgreSQL()
