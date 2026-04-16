import json

import asyncpg
from asyncpg.exceptions import UniqueViolationError

from core.logger.logger import logger
from schemas.jobs import JobCreateRequest, JobUpdateRequest, job_from_row


async def create_job(
    conn: asyncpg.Connection, user_id: int, data: JobCreateRequest
) -> dict:
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO jobs (
                user_id,
                title,
                company,
                location,
                description,
                requirements,
                employment_type,
                seniority_hint,
                remote_policy,
                tech_stack,
                source,
                source_url,
                external_job_id,
                status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13, $14)
            RETURNING
                id,
                user_id,
                title,
                company,
                location,
                description,
                requirements,
                employment_type,
                seniority_hint,
                remote_policy,
                tech_stack,
                ingestion_relevance_score,
                ingestion_relevance_reason,
                ingestion_exploration_kept,
                source,
                source_url,
                external_job_id,
                status,
                created_at,
                updated_at
            """,
            user_id,
            data.title,
            data.company,
            data.location,
            data.description,
            data.requirements,
            data.employment_type,
            data.seniority_hint,
            data.remote_policy,
            json.dumps(data.tech_stack or []),
            data.source,
            data.source_url,
            data.external_job_id,
            data.status,
        )

        return {
            "status": True,
            "message": "Job created successfully",
            "data": {"job": job_from_row(row)},
        }
    except UniqueViolationError:
        return {
            "status": False,
            "message": "Job already exists for this source",
            "data": {},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def list_jobs(
    conn: asyncpg.Connection, user_id: int, limit: int = 20, offset: int = 0
) -> dict:
    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)

    try:
        rows = await conn.fetch(
            """
            SELECT
                id,
                user_id,
                title,
                company,
                location,
                description,
                requirements,
                employment_type,
                seniority_hint,
                remote_policy,
                tech_stack,
                ingestion_relevance_score,
                ingestion_relevance_reason,
                ingestion_exploration_kept,
                source,
                source_url,
                external_job_id,
                status,
                created_at,
                updated_at
            FROM jobs
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            """,
            user_id,
            safe_limit,
            safe_offset,
        )

        jobs = [job_from_row(row) for row in rows]

        return {
            "status": True,
            "message": "Jobs retrieved successfully",
            "data": {
                "jobs": jobs,
                "pagination": {
                    "limit": safe_limit,
                    "offset": safe_offset,
                    "count": len(jobs),
                },
            },
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_one_job(conn: asyncpg.Connection, user_id: int, job_id: int) -> dict:
    try:
        row = await conn.fetchrow(
            """
            SELECT
                id,
                user_id,
                title,
                company,
                location,
                description,
                requirements,
                employment_type,
                seniority_hint,
                remote_policy,
                tech_stack,
                ingestion_relevance_score,
                ingestion_relevance_reason,
                ingestion_exploration_kept,
                source,
                source_url,
                external_job_id,
                status,
                created_at,
                updated_at
            FROM jobs
            WHERE id = $1 AND user_id = $2
            """,
            job_id,
            user_id,
        )

        if not row:
            return {"status": False, "message": "Job not found", "data": {}}

        return {
            "status": True,
            "message": "Job retrieved successfully",
            "data": {"job": job_from_row(row)},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def update_job(
    conn: asyncpg.Connection, user_id: int, job_id: int, data: JobUpdateRequest
) -> dict:
    allowed_columns = {
        "title",
        "company",
        "location",
        "description",
        "requirements",
        "employment_type",
        "seniority_hint",
        "remote_policy",
        "tech_stack",
        "source",
        "source_url",
        "external_job_id",
        "status",
    }
    filtered = {
        key: value
        for key, value in data.model_dump(exclude_none=True).items()
        if key in allowed_columns
    }

    if not filtered:
        return {"status": False, "message": "No fields to update", "data": {}}

    try:
        columns = list(filtered.keys())
        values = []
        set_parts: list[str] = []

        for idx, col in enumerate(columns, 1):
            if col == "tech_stack":
                values.append(json.dumps(filtered[col] or []))
                set_parts.append(f"{col} = ${idx}::jsonb")
            else:
                values.append(filtered[col])
                set_parts.append(f"{col} = ${idx}")

        set_clause = ", ".join(set_parts)

        values.extend([job_id, user_id])

        query = f"""
            UPDATE jobs SET {set_clause}, updated_at = NOW()
            WHERE id = ${len(values) - 1} AND user_id = ${len(values)}
            RETURNING
                id,
                user_id,
                title,
                company,
                location,
                description,
                requirements,
                employment_type,
                seniority_hint,
                remote_policy,
                tech_stack,
                ingestion_relevance_score,
                ingestion_relevance_reason,
                ingestion_exploration_kept,
                source,
                source_url,
                external_job_id,
                status,
                created_at,
                updated_at
        """

        row = await conn.fetchrow(query, *values)

        if not row:
            return {"status": False, "message": "Job not found", "data": {}}

        return {
            "status": True,
            "message": "Job updated successfully",
            "data": {"job": job_from_row(row)},
        }
    except UniqueViolationError:
        return {
            "status": False,
            "message": "Job already exists for this source",
            "data": {},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def delete_job(conn: asyncpg.Connection, user_id: int, job_id: int) -> dict:
    try:
        result = await conn.execute(
            "DELETE FROM jobs WHERE id = $1 AND user_id = $2", job_id, user_id
        )

        if result == "DELETE 0":
            return {"status": False, "message": "Job not found", "data": {}}

        return {
            "status": True,
            "message": "Job deleted successfully",
            "data": {"jobId": job_id},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}
