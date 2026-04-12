import asyncpg

from core.logger.logger import logger
from services.rules import scoring_policy


async def get_job_score(conn: asyncpg.Connection, user_id: int, job_id: int) -> dict:
    try:
        row = await conn.fetchrow(
            """
            SELECT j.id,
                   j.title,
                   j.company,
                   j.status      AS job_status,
                   js.score,
                   js.reason,
                   js.updated_at AS score_updated_at
            FROM jobs j
                     LEFT JOIN job_scores js
                               ON js.job_id = j.id AND js.user_id = j.user_id
            WHERE j.id = $1
              AND j.user_id = $2
            """,
            job_id,
            user_id,
        )

        if not row:
            return {"status": False, "message": "Job not found", "data": {}}

        score = scoring_policy.clamp_score(float(row["score"] or 0))

        return {
            "status": True,
            "message": "Job score retrieved successfully",
            "data": {
                "jobScore": {
                    "jobId": row["id"],
                    "title": row["title"],
                    "company": row["company"],
                    "jobStatus": row["job_status"],
                    "score": round(score, 2),
                    "bucket": scoring_policy.bucket_from_score(score),
                    "reason": row["reason"],
                    "scoreUpdatedAt": (
                        str(row["score_updated_at"]) if row["score_updated_at"] else None
                    ),
                }
            },
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_daily_ranking(
        conn: asyncpg.Connection,
        user_id: int,
        limit: int = 20,
        offset: int = 0,
) -> dict:
    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)

    try:
        rows = await conn.fetch(
            """
            SELECT j.id,
                   j.title,
                   j.company,
                   j.location,
                   j.status      AS job_status,
                   js.score,
                   js.reason,
                   js.updated_at AS score_updated_at
            FROM jobs j
                     LEFT JOIN job_scores js
                               ON js.job_id = j.id AND js.user_id = j.user_id
            WHERE j.user_id = $1
            ORDER BY COALESCE(js.score, 0) DESC, j.created_at DESC
                LIMIT $2
            OFFSET $3
            """,
            user_id,
            safe_limit,
            safe_offset,
        )

        ranking = [
            {
                "rank": index,
                "jobId": row["id"],
                "title": row["title"],
                "company": row["company"],
                "location": row["location"],
                "jobStatus": row["job_status"],
                "score": round(score, 2),
                "bucket": scoring_policy.bucket_from_score(score),
                "reason": row["reason"],
                "scoreUpdatedAt": (
                    row["score_updated_at"].isoformat()
                    if row["score_updated_at"]
                    else None
                ),
            }
            for index, row in enumerate(rows, start=safe_offset + 1)
            for score in [scoring_policy.clamp_score(float(row["score"] or 0))]
        ]

        return {
            "status": True,
            "message": "Daily ranking retrieved successfully",
            "data": {
                "ranking": ranking,
                "pagination": {
                    "limit": safe_limit,
                    "offset": safe_offset,
                    "count": len(ranking),
                },
            },
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "Ranking module base ready",
        "data": {"module": "ranking"},
    }
