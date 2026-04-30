from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.http.http_client import http_client
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from routes.applications.router import router as applications_router
from routes.auth.router import router as auth_router
from routes.digest.router import router as digest_router
from routes.feedback.router import router as feedback_router
from routes.jobs.router import router as jobs_router
from routes.pipeline.router import router as pipeline_router
from routes.users.router import router as users_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting service connections...")

    await postgresql.connect()
    await redis_cache.connect()
    await rabbitmq.connect()
    await http_client.connect()

    logger.info("All services connected successfully.")

    yield

    await postgresql.disconnect()
    await redis_cache.disconnect()
    await rabbitmq.disconnect()
    await http_client.disconnect()


app = FastAPI(
    lifespan=lifespan,
    title="Job Search MVP API",
    docs_url="/docs",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(users_router, prefix="/users", tags=["users"])
app.include_router(pipeline_router, prefix="/pipeline", tags=["pipeline"])
app.include_router(jobs_router, prefix="/jobs", tags=["jobs"])
app.include_router(applications_router, prefix="/applications", tags=["applications"])
app.include_router(feedback_router, prefix="/feedback", tags=["feedback"])
app.include_router(digest_router, prefix="/digest", tags=["digest"])
