from fastapi import Depends, HTTPException, Request

from core.config.config import (
    RATE_LIMIT_FORGET_PASSWORD_MAX_REQUESTS,
    RATE_LIMIT_FORGET_PASSWORD_WINDOW_SECONDS,
    RATE_LIMIT_LOGIN_MAX_REQUESTS,
    RATE_LIMIT_LOGIN_WINDOW_SECONDS,
    RATE_LIMIT_VALIDATE_CODE_MAX_REQUESTS,
    RATE_LIMIT_VALIDATE_CODE_WINDOW_SECONDS,
)
from core.redis.redis import redis_cache


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def rate_limiter(max_requests: int, window_seconds: int):
    async def dependency(
        request: Request, redis_client=Depends(redis_cache.get_redis)
    ):
        ip = _client_ip(request)
        key = f"rate_limit:{request.url.path}:{ip}"

        current = await redis_client.incr(key)
        if current == 1:
            await redis_client.expire(key, window_seconds)

        if current > max_requests:
            raise HTTPException(
                status_code=429,
                detail="Too many requests. Please try again later.",
            )

        return True

    return dependency


LOGIN_RATE_LIMIT_DEPS = [
    Depends(rate_limiter(RATE_LIMIT_LOGIN_MAX_REQUESTS, RATE_LIMIT_LOGIN_WINDOW_SECONDS))
]
FORGET_PASSWORD_RATE_LIMIT_DEPS = [
    Depends(
        rate_limiter(
            RATE_LIMIT_FORGET_PASSWORD_MAX_REQUESTS,
            RATE_LIMIT_FORGET_PASSWORD_WINDOW_SECONDS,
        )
    )
]
VALIDATE_CODE_RATE_LIMIT_DEPS = [
    Depends(
        rate_limiter(
            RATE_LIMIT_VALIDATE_CODE_MAX_REQUESTS,
            RATE_LIMIT_VALIDATE_CODE_WINDOW_SECONDS,
        )
    )
]
