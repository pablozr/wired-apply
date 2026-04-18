from datetime import date, timedelta

import asyncpg

from core.logger.logger import logger
from core.utils.json_utils import ensure_dict
from services.rules import scoring_policy


def _coerce_date(value) -> date | None:
    if isinstance(value, date):
        return value

    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None

        try:
            return date.fromisoformat(normalized)
        except ValueError:
            return None

    return None


def _resolve_ranking_window(
    days_range: int,
    date_from: date | str | None,
    date_to: date | str | None,
) -> tuple[date, date] | None:
    parsed_date_from = _coerce_date(date_from)
    parsed_date_to = _coerce_date(date_to)
    has_date_from = parsed_date_from is not None
    has_date_to = parsed_date_to is not None

    if has_date_from != has_date_to:
        return None

    if has_date_from and has_date_to:
        if parsed_date_from > parsed_date_to:
            return None

        days_span = (parsed_date_to - parsed_date_from).days + 1
        if days_span > 30:
            return None

        return parsed_date_from, parsed_date_to

    try:
        safe_days_range = max(1, min(int(days_range), 30))
    except (TypeError, ValueError):
        safe_days_range = 7

    window_to = date.today()
    window_from = window_to - timedelta(days=safe_days_range - 1)
    return window_from, window_to


async def get_job_score(conn: asyncpg.Connection, user_id: int, job_id: int) -> dict:
    try:
        row = await conn.fetchrow(
            """
            SELECT j.id,
                   j.title,
                   j.company,
                   j.source_posted_at,
                   j.first_seen_at,
                   j.last_seen_at,
                   COALESCE(j.source_posted_at, j.first_seen_at) AS effective_date,
                   j.status      AS job_status,
                   js.score,
                   js.deterministic_score,
                   js.ai_score,
                   js.ai_confidence,
                    js.final_score,
                    js.reason,
                    js.ai_reason,
                    js.ai_breakdown,
                    js.ai_skipped_reason,
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

        score = scoring_policy.clamp_score(
            float(row["final_score"] or row["score"] or 0)
        )
        deterministic_score = row["deterministic_score"]
        ai_score = row["ai_score"]
        ai_confidence = row["ai_confidence"]

        return {
            "status": True,
            "message": "Job score retrieved successfully",
            "data": {
                "jobScore": {
                    "jobId": row["id"],
                    "title": row["title"],
                    "company": row["company"],
                    "sourcePostedAt": (
                        row["source_posted_at"].isoformat()
                        if row["source_posted_at"]
                        else None
                    ),
                    "firstSeenAt": (
                        row["first_seen_at"].isoformat()
                        if row["first_seen_at"]
                        else None
                    ),
                    "lastSeenAt": (
                        row["last_seen_at"].isoformat()
                        if row["last_seen_at"]
                        else None
                    ),
                    "effectiveDate": (
                        row["effective_date"].isoformat()
                        if row["effective_date"]
                        else None
                    ),
                    "jobStatus": row["job_status"],
                    "score": round(score, 2),
                    "deterministicScore": (
                        round(float(deterministic_score), 2)
                        if deterministic_score is not None
                        else None
                    ),
                    "aiScore": (
                        round(float(ai_score), 2) if ai_score is not None else None
                    ),
                    "aiConfidence": (
                        round(float(ai_confidence), 2)
                        if ai_confidence is not None
                        else None
                    ),
                    "bucket": scoring_policy.bucket_from_score(score),
                    "reason": row["reason"],
                    "aiReason": row["ai_reason"],
                    "aiSkippedReason": row["ai_skipped_reason"],
                    "aiBreakdown": ensure_dict(row["ai_breakdown"]),
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
    days_range: int = 7,
    date_from: date | str | None = None,
    date_to: date | str | None = None,
) -> dict:
    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)
    ranking_window = _resolve_ranking_window(days_range, date_from, date_to)

    if ranking_window is None:
        return {
            "status": False,
            "message": "Invalid ranking window. Use daysRange or dateFrom/dateTo with max 30 days",
            "data": {},
        }

    window_from, window_to = ranking_window
    resolved_days_range = (window_to - window_from).days + 1

    try:
        rows = await conn.fetch(
            """
            SELECT j.id,
                    j.title,
                    j.company,
                    j.location,
                    j.source_posted_at,
                    j.first_seen_at,
                    j.last_seen_at,
                    COALESCE(j.source_posted_at, j.first_seen_at) AS effective_date,
                    j.status      AS job_status,
                    js.score,
                    js.deterministic_score,
                    js.ai_score,
                    js.ai_confidence,
                    js.final_score,
                    js.reason,
                    js.ai_reason,
                    js.ai_breakdown,
                    js.ai_skipped_reason,
                    js.updated_at AS score_updated_at
            FROM jobs j
                     LEFT JOIN job_scores js
                               ON js.job_id = j.id AND js.user_id = j.user_id
            WHERE j.user_id = $1
              AND COALESCE(j.source_posted_at, j.first_seen_at)::date BETWEEN $2::date AND $3::date
            ORDER BY COALESCE(js.final_score, js.score, 0) DESC, j.created_at DESC
                LIMIT $4
            OFFSET $5
            """,
            user_id,
            window_from,
            window_to,
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
                "sourcePostedAt": (
                    row["source_posted_at"].isoformat()
                    if row["source_posted_at"]
                    else None
                ),
                "firstSeenAt": (
                    row["first_seen_at"].isoformat()
                    if row["first_seen_at"]
                    else None
                ),
                "lastSeenAt": (
                    row["last_seen_at"].isoformat()
                    if row["last_seen_at"]
                    else None
                ),
                "effectiveDate": (
                    row["effective_date"].isoformat()
                    if row["effective_date"]
                    else None
                ),
                "jobStatus": row["job_status"],
                "score": round(score, 2),
                "deterministicScore": (
                    round(float(row["deterministic_score"]), 2)
                    if row["deterministic_score"] is not None
                    else None
                ),
                "aiScore": (
                    round(float(row["ai_score"]), 2)
                    if row["ai_score"] is not None
                    else None
                ),
                "aiConfidence": (
                    round(float(row["ai_confidence"]), 2)
                    if row["ai_confidence"] is not None
                    else None
                ),
                "bucket": scoring_policy.bucket_from_score(score),
                "reason": row["reason"],
                "aiReason": row["ai_reason"],
                "aiSkippedReason": row["ai_skipped_reason"],
                "aiBreakdown": ensure_dict(row["ai_breakdown"]),
                "scoreUpdatedAt": (
                    row["score_updated_at"].isoformat()
                    if row["score_updated_at"]
                    else None
                ),
            }
            for index, row in enumerate(rows, start=safe_offset + 1)
            for score in [
                scoring_policy.clamp_score(
                    float(row["final_score"] or row["score"] or 0)
                )
            ]
        ]

        return {
            "status": True,
            "message": "Daily ranking retrieved successfully",
            "data": {
                "ranking": ranking,
                "window": {
                    "dateFrom": window_from.isoformat(),
                    "dateTo": window_to.isoformat(),
                    "daysRange": resolved_days_range,
                },
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
