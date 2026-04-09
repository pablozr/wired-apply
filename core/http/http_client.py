from typing import Optional

import httpx

from core.config.config import (
    HTTP_CLIENT_MAX_CONNECTIONS,
    HTTP_CLIENT_MAX_KEEPALIVE_CONNECTIONS,
    HTTP_CLIENT_TIMEOUT_SECONDS,
)


class HTTPClient:
    client: Optional[httpx.AsyncClient] = None

    async def connect(self):
        if self.client is not None:
            return

        limits = httpx.Limits(
            max_connections=HTTP_CLIENT_MAX_CONNECTIONS,
            max_keepalive_connections=HTTP_CLIENT_MAX_KEEPALIVE_CONNECTIONS,
        )
        timeout = httpx.Timeout(HTTP_CLIENT_TIMEOUT_SECONDS)

        self.client = httpx.AsyncClient(timeout=timeout, limits=limits)

    async def disconnect(self):
        if self.client is None:
            return

        await self.client.aclose()
        self.client = None

    async def get_http_client(self):
        if self.client is None:
            await self.connect()

        yield self.client


http_client = HTTPClient()
