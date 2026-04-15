import asyncio
import hashlib
from typing import Any, Awaitable, Callable

import httpx

from core.config.config import (
    ATS_ASHBY_ORGANIZATIONS,
    ATS_ENABLE_MOCK_FALLBACK,
    ATS_GREENHOUSE_BOARDS,
    ATS_LEVER_COMPANIES,
    ATS_MAX_JOBS_PER_SOURCE,
)
from core.http.http_client import http_client
from core.logger.logger import logger


def _parse_handles(raw_value: str) -> list[str]:
    handles: list[str] = []
    seen: set[str] = set()

    for item in raw_value.split(","):
        normalized = item.strip().lower()
        if not normalized or normalized in seen:
            continue

        handles.append(normalized)
        seen.add(normalized)

    return handles


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, str):
        normalized = value.strip()
    else:
        normalized = str(value).strip()

    return normalized or None


def _stable_external_job_id(
    provider: str,
    target: str,
    raw_id: Any,
    source_url: str | None,
    title: str,
) -> str:
    raw_id_text = _clean_text(raw_id)
    if raw_id_text:
        return f"{target}:{raw_id_text}".lower()

    seed = f"{provider}|{target}|{source_url or ''}|{title.lower()}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{target}:{digest}".lower()


def _build_mock_jobs(force: bool) -> list[dict]:
    jobs = [
        {
            "title": "Backend Engineer",
            "company": "Acme Labs",
            "location": "Remote",
            "source": "ingestion",
            "source_url": "https://jobs.example.com/backend-engineer",
            "external_job_id": "wa-backend-engineer",
        },
        {
            "title": "Python Developer",
            "company": "Orbit Systems",
            "location": "Sao Paulo",
            "source": "ingestion",
            "source_url": "https://jobs.example.com/python-developer",
            "external_job_id": "wa-python-developer",
        },
        {
            "title": "Data Engineer",
            "company": "Nova Data",
            "location": "Remote",
            "source": "ingestion",
            "source_url": "https://jobs.example.com/data-engineer",
            "external_job_id": "wa-data-engineer",
        },
    ]

    if force:
        jobs.append(
            {
                "title": "Site Reliability Engineer",
                "company": "Atlas Cloud",
                "location": "Remote",
                "source": "ingestion",
                "source_url": "https://jobs.example.com/sre",
                "external_job_id": "wa-sre-engineer",
            }
        )

    return jobs


async def _get_http_client() -> httpx.AsyncClient:
    if http_client.client is None:
        await http_client.connect()

    if http_client.client is None:
        raise RuntimeError("HTTP client is not connected")

    return http_client.client


async def _fetch_greenhouse_jobs(
    board_token: str,
    client: httpx.AsyncClient,
    max_jobs: int,
) -> list[dict]:
    response = await client.get(
        f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs",
        params={"content": "false"},
    )
    response.raise_for_status()

    payload = response.json()
    raw_jobs = payload.get("jobs") if isinstance(payload, dict) else []
    if not isinstance(raw_jobs, list):
        return []

    jobs: list[dict] = []
    for raw_job in raw_jobs[:max_jobs]:
        if not isinstance(raw_job, dict):
            continue

        title = _clean_text(raw_job.get("title"))
        if not title:
            continue

        company = _clean_text(raw_job.get("company_name")) or board_token

        location_value = raw_job.get("location")
        if isinstance(location_value, dict):
            location = _clean_text(location_value.get("name"))
        else:
            location = _clean_text(location_value)

        source_url = _clean_text(raw_job.get("absolute_url"))
        external_job_id = _stable_external_job_id(
            "greenhouse",
            board_token,
            raw_job.get("id"),
            source_url,
            title,
        )

        jobs.append(
            {
                "title": title,
                "company": company,
                "location": location,
                "source": "greenhouse",
                "source_url": source_url,
                "external_job_id": external_job_id,
            }
        )

    return jobs


async def _fetch_lever_jobs(
    company_handle: str,
    client: httpx.AsyncClient,
    max_jobs: int,
) -> list[dict]:
    response = await client.get(
        f"https://api.lever.co/v0/postings/{company_handle}",
        params={"mode": "json"},
    )
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list):
        return []

    jobs: list[dict] = []
    for raw_job in payload[:max_jobs]:
        if not isinstance(raw_job, dict):
            continue

        title = _clean_text(raw_job.get("text"))
        if not title:
            continue

        categories = raw_job.get("categories")
        if isinstance(categories, dict):
            location = _clean_text(categories.get("location"))
        else:
            location = None

        source_url = _clean_text(raw_job.get("hostedUrl")) or _clean_text(
            raw_job.get("applyUrl")
        )
        external_job_id = _stable_external_job_id(
            "lever",
            company_handle,
            raw_job.get("id"),
            source_url,
            title,
        )

        jobs.append(
            {
                "title": title,
                "company": company_handle,
                "location": location,
                "source": "lever",
                "source_url": source_url,
                "external_job_id": external_job_id,
            }
        )

    return jobs


async def _fetch_ashby_jobs(
    organization: str,
    client: httpx.AsyncClient,
    max_jobs: int,
) -> list[dict]:
    response = await client.get(
        f"https://api.ashbyhq.com/posting-api/job-board/{organization}"
    )
    response.raise_for_status()

    payload = response.json()
    raw_jobs = payload.get("jobs") if isinstance(payload, dict) else []
    if not isinstance(raw_jobs, list):
        return []

    jobs: list[dict] = []
    for raw_job in raw_jobs[:max_jobs]:
        if not isinstance(raw_job, dict):
            continue

        title = _clean_text(raw_job.get("title"))
        if not title:
            continue

        location = _clean_text(raw_job.get("location"))
        source_url = _clean_text(raw_job.get("jobUrl")) or _clean_text(
            raw_job.get("applyUrl")
        )
        external_job_id = _stable_external_job_id(
            "ashby",
            organization,
            raw_job.get("id"),
            source_url,
            title,
        )

        jobs.append(
            {
                "title": title,
                "company": organization,
                "location": location,
                "source": "ashby",
                "source_url": source_url,
                "external_job_id": external_job_id,
            }
        )

    return jobs


async def _fetch_target(
    provider: str,
    target: str,
    fetcher: Callable[[str, httpx.AsyncClient, int], Awaitable[list[dict]]],
    client: httpx.AsyncClient,
    max_jobs: int,
) -> dict:
    try:
        jobs = await fetcher(target, client, max_jobs)
        return {
            "provider": provider,
            "target": target,
            "status": "ok",
            "jobsCount": len(jobs),
            "jobs": jobs,
        }
    except httpx.HTTPStatusError as error:
        status_code = error.response.status_code if error.response else None
        logger.warning(
            "ats_fetch_http_error provider=%s target=%s status=%s",
            provider,
            target,
            status_code,
        )
    except httpx.HTTPError as error:
        logger.warning(
            "ats_fetch_transport_error provider=%s target=%s error=%s",
            provider,
            target,
            error,
        )
    except Exception as error:
        logger.exception(error)

    return {
        "provider": provider,
        "target": target,
        "status": "error",
        "jobsCount": 0,
        "jobs": [],
    }


def _dedupe_jobs(jobs: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen_keys: set[str] = set()

    for job in jobs:
        source = _clean_text(job.get("source")) or "unknown"
        external_job_id = _clean_text(job.get("external_job_id"))

        if external_job_id:
            key = f"{source}:{external_job_id}".lower()
        else:
            title = _clean_text(job.get("title")) or ""
            company = _clean_text(job.get("company")) or ""
            source_url = _clean_text(job.get("source_url")) or ""
            key_seed = f"{source}|{title}|{company}|{source_url}".lower()
            key = hashlib.sha1(key_seed.encode("utf-8")).hexdigest()

        if key in seen_keys:
            continue

        deduped.append(job)
        seen_keys.add(key)

    return deduped


async def fetch_jobs(force: bool = False) -> dict:
    greenhouse_boards = _parse_handles(ATS_GREENHOUSE_BOARDS)
    lever_companies = _parse_handles(ATS_LEVER_COMPANIES)
    ashby_organizations = _parse_handles(ATS_ASHBY_ORGANIZATIONS)

    configured_targets: list[tuple[str, str, Callable[[str, httpx.AsyncClient, int], Awaitable[list[dict]]]]] = []
    configured_targets.extend(
        ("greenhouse", board, _fetch_greenhouse_jobs) for board in greenhouse_boards
    )
    configured_targets.extend(
        ("lever", company, _fetch_lever_jobs) for company in lever_companies
    )
    configured_targets.extend(
        ("ashby", organization, _fetch_ashby_jobs)
        for organization in ashby_organizations
    )

    if not configured_targets:
        if ATS_ENABLE_MOCK_FALLBACK:
            jobs = _build_mock_jobs(force)
            return {
                "status": True,
                "message": "No ATS source configured. Using mock ingestion jobs",
                "data": {
                    "jobs": jobs,
                    "sources": [],
                    "fallbackUsed": True,
                    "configuredSources": 0,
                    "successfulSources": 0,
                },
            }

        return {
            "status": False,
            "message": "No ATS source configured",
            "data": {},
        }

    try:
        max_jobs = max(1, int(ATS_MAX_JOBS_PER_SOURCE))
        client = await _get_http_client()

        tasks = [
            _fetch_target(provider, target, fetcher, client, max_jobs)
            for provider, target, fetcher in configured_targets
        ]
        source_results = await asyncio.gather(*tasks)

        jobs: list[dict] = []
        source_metadata: list[dict] = []
        for result in source_results:
            jobs.extend(result["jobs"])
            source_metadata.append(
                {
                    "provider": result["provider"],
                    "target": result["target"],
                    "status": result["status"],
                    "jobsCount": result["jobsCount"],
                }
            )

        jobs = _dedupe_jobs(jobs)

        fallback_used = False
        if not jobs and ATS_ENABLE_MOCK_FALLBACK:
            jobs = _build_mock_jobs(force)
            fallback_used = True

        if not jobs:
            return {
                "status": False,
                "message": "ATS fetch finished with no jobs",
                "data": {},
            }

        successful_sources = sum(
            1 for source in source_metadata if source["status"] == "ok"
        )

        message = f"Fetched {len(jobs)} jobs from ATS sources"
        if fallback_used:
            message = f"{message}; mock fallback enabled"

        return {
            "status": True,
            "message": message,
            "data": {
                "jobs": jobs,
                "sources": source_metadata,
                "fallbackUsed": fallback_used,
                "configuredSources": len(configured_targets),
                "successfulSources": successful_sources,
            },
        }
    except Exception as error:
        logger.exception(error)

        if ATS_ENABLE_MOCK_FALLBACK:
            jobs = _build_mock_jobs(force)
            return {
                "status": True,
                "message": "ATS fetch failed. Using mock ingestion jobs",
                "data": {
                    "jobs": jobs,
                    "sources": [],
                    "fallbackUsed": True,
                    "configuredSources": len(configured_targets),
                    "successfulSources": 0,
                },
            }

        return {
            "status": False,
            "message": "Failed to fetch jobs from ATS",
            "data": {},
        }


async def get_module_status() -> dict:
    greenhouse_boards = _parse_handles(ATS_GREENHOUSE_BOARDS)
    lever_companies = _parse_handles(ATS_LEVER_COMPANIES)
    ashby_organizations = _parse_handles(ATS_ASHBY_ORGANIZATIONS)

    return {
        "status": True,
        "message": "ATS integration ready",
        "data": {
            "module": "ats",
            "sourcesConfigured": {
                "greenhouse": len(greenhouse_boards),
                "lever": len(lever_companies),
                "ashby": len(ashby_organizations),
            },
            "mockFallbackEnabled": ATS_ENABLE_MOCK_FALLBACK,
            "maxJobsPerSource": ATS_MAX_JOBS_PER_SOURCE,
        },
    }
