import json
from datetime import datetime

import asyncpg

from services.rules import deduplication_policy


def _normalize_text(value) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


def _normalize_stack(value) -> list[str]:
    if not isinstance(value, list):
        return []

    stack: list[str] = []
    seen: set[str] = set()
    for item in value:
        token = str(item).strip()
        if not token:
            continue

        dedupe_key = token.lower()
        if dedupe_key in seen:
            continue

        stack.append(token)
        seen.add(dedupe_key)

    return stack


def _normalize_source_posted_at(value) -> datetime | None:
    if isinstance(value, datetime):
        return value

    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None

    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize_job(raw_job: dict) -> dict:
    title = _normalize_text(raw_job.get("title")) or "Unknown Role"
    company = _normalize_text(raw_job.get("company")) or "Unknown Company"
    source = (_normalize_text(raw_job.get("source")) or "ingestion").lower()
    external_job_id = _normalize_text(raw_job.get("external_job_id"))

    return {
        "title": title,
        "company": company,
        "location": _normalize_text(raw_job.get("location")),
        "description": _normalize_text(raw_job.get("description")),
        "requirements": _normalize_text(raw_job.get("requirements")),
        "employment_type": _normalize_text(raw_job.get("employment_type")),
        "seniority_hint": _normalize_text(raw_job.get("seniority_hint")),
        "remote_policy": _normalize_text(raw_job.get("remote_policy")),
        "tech_stack": _normalize_stack(raw_job.get("tech_stack")),
        "source": source,
        "source_url": _normalize_text(raw_job.get("source_url")),
        "external_job_id": external_job_id,
        "source_posted_at": _normalize_source_posted_at(
            raw_job.get("source_posted_at")
        ),
    }


async def upsert_global_job(
    conn: asyncpg.Connection,
    raw_job: dict,
    source_target: str | None = None,
) -> int:
    normalized_job = _normalize_job(raw_job)
    dedupe_key = deduplication_policy.dedupe_key(
        normalized_job["source"],
        normalized_job["external_job_id"],
        normalized_job["title"],
        normalized_job["company"],
    )
    external_job_id = normalized_job["external_job_id"] or dedupe_key
    source_target_value = _normalize_text(source_target)
    if source_target_value is None:
        source_target_value = _normalize_text(raw_job.get("source_target"))

    global_job_row = await conn.fetchrow(
        """
        INSERT INTO global_jobs (
            dedupe_key,
            title,
            company,
            location,
            description,
            requirements,
            employment_type,
            seniority_hint,
            remote_policy,
            tech_stack,
            source_posted_at,
            first_seen_at,
            last_seen_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10::jsonb,
            $11,
            NOW(),
            NOW()
        )
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
            title = EXCLUDED.title,
            company = EXCLUDED.company,
            location = EXCLUDED.location,
            description = EXCLUDED.description,
            requirements = EXCLUDED.requirements,
            employment_type = EXCLUDED.employment_type,
            seniority_hint = EXCLUDED.seniority_hint,
            remote_policy = EXCLUDED.remote_policy,
            tech_stack = EXCLUDED.tech_stack,
            source_posted_at = COALESCE(EXCLUDED.source_posted_at, global_jobs.source_posted_at),
            last_seen_at = NOW(),
            updated_at = NOW()
        RETURNING id
        """,
        dedupe_key,
        normalized_job["title"],
        normalized_job["company"],
        normalized_job["location"],
        normalized_job["description"],
        normalized_job["requirements"],
        normalized_job["employment_type"],
        normalized_job["seniority_hint"],
        normalized_job["remote_policy"],
        json.dumps(normalized_job["tech_stack"]),
        normalized_job["source_posted_at"],
    )

    global_job_id = int(global_job_row["id"])

    await conn.execute(
        """
        INSERT INTO global_job_sources (
            global_job_id,
            source,
            source_target,
            source_url,
            external_job_id,
            source_posted_at,
            first_seen_at,
            last_seen_at,
            raw_payload
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            NOW(),
            NOW(),
            $7::jsonb
        )
        ON CONFLICT (source, external_job_id)
        DO UPDATE SET
            global_job_id = EXCLUDED.global_job_id,
            source_target = EXCLUDED.source_target,
            source_url = EXCLUDED.source_url,
            source_posted_at = COALESCE(EXCLUDED.source_posted_at, global_job_sources.source_posted_at),
            raw_payload = EXCLUDED.raw_payload,
            last_seen_at = NOW(),
            updated_at = NOW()
        """,
        global_job_id,
        normalized_job["source"],
        source_target_value,
        normalized_job["source_url"],
        external_job_id,
        normalized_job["source_posted_at"],
        json.dumps(raw_job, default=str),
    )

    return global_job_id
