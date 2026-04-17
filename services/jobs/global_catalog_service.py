from datetime import date

import asyncpg

from core.logger.logger import logger


async def list_jobs_by_window(
    conn: asyncpg.Connection,
    date_from: date,
    date_to: date,
    limit: int = 1500,
) -> dict:
    safe_limit = max(1, min(int(limit), 5000))

    try:
        rows = await conn.fetch(
            """
            SELECT
                gj.id,
                gj.dedupe_key,
                gj.title,
                gj.company,
                gj.location,
                gj.description,
                gj.requirements,
                gj.employment_type,
                gj.seniority_hint,
                gj.remote_policy,
                gj.tech_stack,
                gj.source_posted_at,
                src.source,
                src.source_target,
                src.source_url,
                src.external_job_id,
                COALESCE(gj.source_posted_at, gj.first_seen_at) AS effective_date
            FROM global_jobs gj
            LEFT JOIN LATERAL (
                SELECT
                    gjs.source,
                    gjs.source_target,
                    gjs.source_url,
                    gjs.external_job_id,
                    gjs.source_posted_at
                FROM global_job_sources gjs
                WHERE gjs.global_job_id = gj.id
                ORDER BY gjs.last_seen_at DESC, gjs.id DESC
                LIMIT 1
            ) src ON TRUE
            WHERE COALESCE(gj.source_posted_at, gj.first_seen_at)::date BETWEEN $1::date AND $2::date
            ORDER BY effective_date DESC, gj.last_seen_at DESC
            LIMIT $3
            """,
            date_from,
            date_to,
            safe_limit,
        )

        jobs: list[dict] = []
        sources_seen: set[str] = set()

        for row in rows:
            source = str(row["source"] or "global").strip().lower() or "global"
            source_target = row["source_target"]
            source_url = row["source_url"]
            external_job_id = row["external_job_id"]
            source_posted_at = row["source_posted_at"]

            jobs.append(
                {
                    "title": row["title"],
                    "company": row["company"],
                    "location": row["location"],
                    "description": row["description"],
                    "requirements": row["requirements"],
                    "employment_type": row["employment_type"],
                    "seniority_hint": row["seniority_hint"],
                    "remote_policy": row["remote_policy"],
                    "tech_stack": row["tech_stack"] or [],
                    "source": source,
                    "source_target": source_target,
                    "source_url": source_url,
                    "external_job_id": external_job_id or row["dedupe_key"],
                    "source_posted_at": (
                        source_posted_at.isoformat() if source_posted_at else None
                    ),
                }
            )

            sources_seen.add(source)

        return {
            "status": True,
            "message": "Global catalog jobs retrieved successfully",
            "data": {
                "jobs": jobs,
                "sources": sorted(sources_seen),
                "fallbackUsed": False,
                "window": {
                    "dateFrom": date_from.isoformat(),
                    "dateTo": date_to.isoformat(),
                },
            },
        }
    except Exception as e:
        logger.exception(e)
        return {
            "status": False,
            "message": "Failed to query global catalog",
            "data": {},
        }
