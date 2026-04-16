import asyncio
import html
import hashlib
import re
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


TECH_STACK_KEYWORDS = [
    "python",
    "django",
    "fastapi",
    "flask",
    "postgresql",
    "mysql",
    "redis",
    "rabbitmq",
    "docker",
    "kubernetes",
    "aws",
    "gcp",
    "azure",
    "javascript",
    "typescript",
    "react",
    "node",
    "golang",
    "java",
    "spring",
    "c#",
    ".net",
    "terraform",
    "playwright",
]


def _plain_text(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None

    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _split_tokens(value: str | None) -> list[str]:
    if not value:
        return []

    tokens: list[str] = []
    seen: set[str] = set()
    for token in re.split(r"[,;/|\n]+", value):
        normalized = token.strip()
        if not normalized:
            continue

        dedupe_key = normalized.lower()
        if dedupe_key in seen:
            continue

        tokens.append(normalized)
        seen.add(dedupe_key)

    return tokens


def _extract_tech_stack(*texts: str | None) -> list[str]:
    combined = " ".join([text for text in texts if text]).lower()
    if not combined:
        return []

    stack: list[str] = []
    seen: set[str] = set()

    for keyword in TECH_STACK_KEYWORDS:
        if keyword in combined and keyword not in seen:
            stack.append(keyword)
            seen.add(keyword)

    return stack


def _extract_requirements_from_text(description: str | None) -> str | None:
    text = _clean_text(description)
    if not text:
        return None

    normalized = text.lower()
    section_markers = [
        "required skills",
        "requirements",
        "qualifications",
        "what we are looking for",
        "what you'll bring",
        "must have",
        "you have",
    ]
    stop_markers = [
        "nice to have",
        "benefits",
        "about ",
        "what success looks like",
        "how to apply",
    ]

    for marker in section_markers:
        start = normalized.find(marker)
        if start < 0:
            continue

        end = len(text)
        for stop_marker in stop_markers:
            stop = normalized.find(stop_marker, start + len(marker))
            if stop > 0:
                end = min(end, stop)

        section = _clean_text(text[start:end])
        if section and len(section) >= 40:
            return section[:3000]

    return None


def _infer_seniority_hint(
    title: str,
    description: str | None,
    explicit_hint: str | None = None,
) -> str | None:
    explicit = _clean_text(explicit_hint)
    if explicit:
        return explicit.upper()

    text = f"{title} {description or ''}".lower()
    if any(token in text for token in ["staff", "principal", "architect"]):
        return "STAFF"
    if any(token in text for token in ["lead", "manager", "head"]):
        return "LEAD"
    if any(token in text for token in ["senior", " sr ", " sr."]):
        return "SENIOR"
    if any(token in text for token in ["junior", "entry level", "intern", "trainee"]):
        return "JUNIOR"
    if any(token in text for token in ["mid", "middle", "pleno"]):
        return "MID"

    return None


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
            "description": "Build and maintain backend services in Python.",
            "requirements": "Python, APIs, SQL.",
            "employment_type": "Full-time",
            "seniority_hint": "MID",
            "remote_policy": "REMOTE",
            "tech_stack": ["python", "postgresql", "docker"],
            "source": "ingestion",
            "source_url": "https://jobs.example.com/backend-engineer",
            "external_job_id": "wa-backend-engineer",
        },
        {
            "title": "Python Developer",
            "company": "Orbit Systems",
            "location": "Sao Paulo",
            "description": "Develop internal APIs and integrations in Python.",
            "requirements": "Python, FastAPI, PostgreSQL.",
            "employment_type": "Full-time",
            "seniority_hint": "JUNIOR",
            "remote_policy": "HYBRID",
            "tech_stack": ["python", "fastapi", "postgresql"],
            "source": "ingestion",
            "source_url": "https://jobs.example.com/python-developer",
            "external_job_id": "wa-python-developer",
        },
        {
            "title": "Data Engineer",
            "company": "Nova Data",
            "location": "Remote",
            "description": "Create data pipelines and ETL workflows.",
            "requirements": "Python, SQL, cloud data tools.",
            "employment_type": "Full-time",
            "seniority_hint": "MID",
            "remote_policy": "REMOTE",
            "tech_stack": ["python", "sql", "aws"],
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
                "description": "Operate distributed systems and improve reliability.",
                "requirements": "Linux, cloud, observability.",
                "employment_type": "Full-time",
                "seniority_hint": "SENIOR",
                "remote_policy": "REMOTE",
                "tech_stack": ["kubernetes", "aws", "python"],
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
        params={"content": "true"},
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

        description = _plain_text(raw_job.get("content"))

        employment_type = None
        seniority_hint = None
        remote_policy = None
        requirements_chunks: list[str] = []
        metadata_stack: list[str] = []

        metadata = raw_job.get("metadata")
        if isinstance(metadata, list):
            for item in metadata:
                if not isinstance(item, dict):
                    continue

                meta_name = (_clean_text(item.get("name")) or "").lower()
                meta_value = _plain_text(item.get("value"))
                if not meta_name or not meta_value:
                    continue

                if (
                    "require" in meta_name
                    or "qualification" in meta_name
                    or "must" in meta_name
                    or "responsibil" in meta_name
                ):
                    requirements_chunks.append(meta_value)

                if employment_type is None and (
                    "employment" in meta_name
                    or "commitment" in meta_name
                    or "contract" in meta_name
                    or "type" in meta_name
                ):
                    employment_type = meta_value

                if seniority_hint is None and (
                    "seniority" in meta_name
                    or "level" in meta_name
                    or "experience" in meta_name
                ):
                    seniority_hint = meta_value

                if remote_policy is None and (
                    "remote" in meta_name
                    or "work model" in meta_name
                    or "workplace" in meta_name
                    or "location type" in meta_name
                ):
                    remote_policy = meta_value

                if (
                    "stack" in meta_name
                    or "skill" in meta_name
                    or "technology" in meta_name
                    or "language" in meta_name
                    or "framework" in meta_name
                ):
                    metadata_stack.extend(_split_tokens(meta_value))

        requirements = _clean_text(" ".join(requirements_chunks))
        if requirements is None:
            requirements = _extract_requirements_from_text(description)
        if requirements:
            requirements = requirements[:3000]

        seniority_hint = _infer_seniority_hint(title, description, seniority_hint)

        tech_stack = _extract_tech_stack(description, requirements, " ".join(metadata_stack))
        for token in metadata_stack:
            normalized = token.lower()
            if normalized and normalized not in tech_stack:
                tech_stack.append(normalized)

        location_text = (location or "").lower()
        if remote_policy is None:
            if "remote" in location_text:
                remote_policy = "REMOTE"
            elif "hybrid" in location_text:
                remote_policy = "HYBRID"

        jobs.append(
            {
                "title": title,
                "company": company,
                "location": location,
                "description": description,
                "requirements": requirements,
                "employment_type": employment_type,
                "seniority_hint": seniority_hint,
                "remote_policy": remote_policy,
                "tech_stack": tech_stack,
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

        description = _plain_text(raw_job.get("descriptionPlain")) or _plain_text(
            raw_job.get("description")
        )

        requirements_chunks: list[str] = []
        lists_payload = raw_job.get("lists")
        if isinstance(lists_payload, list):
            for section in lists_payload:
                if not isinstance(section, dict):
                    continue

                section_title = (_clean_text(section.get("text")) or "").lower()
                section_content = _plain_text(section.get("content"))
                if not section_content:
                    continue

                if (
                    "require" in section_title
                    or "qualification" in section_title
                    or "skill" in section_title
                    or "responsibil" in section_title
                ):
                    requirements_chunks.append(section_content)

        requirements = _clean_text(" ".join(requirements_chunks))
        if requirements is None:
            requirements = _extract_requirements_from_text(description)
        if requirements:
            requirements = requirements[:3000]

        employment_type = None
        seniority_hint = None
        if isinstance(categories, dict):
            employment_type = _clean_text(categories.get("commitment"))
            seniority_hint = _clean_text(categories.get("level"))

        seniority_hint = _infer_seniority_hint(title, description, seniority_hint)

        remote_policy = _clean_text(raw_job.get("workplaceType"))
        if remote_policy is None and isinstance(categories, dict):
            location_hint = (_clean_text(categories.get("location")) or "").lower()
            if "remote" in location_hint:
                remote_policy = "REMOTE"
            elif "hybrid" in location_hint:
                remote_policy = "HYBRID"

        tech_stack = _extract_tech_stack(description, requirements, title)

        jobs.append(
            {
                "title": title,
                "company": company_handle,
                "location": location,
                "description": description,
                "requirements": requirements,
                "employment_type": employment_type,
                "seniority_hint": seniority_hint,
                "remote_policy": remote_policy,
                "tech_stack": tech_stack,
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

        description = (
            _plain_text(raw_job.get("descriptionPlain"))
            or _plain_text(raw_job.get("description"))
            or _plain_text(raw_job.get("descriptionHtml"))
        )
        requirements = _plain_text(raw_job.get("requirements")) or _plain_text(
            raw_job.get("jobRequirements")
        )
        if requirements is None:
            requirements = _extract_requirements_from_text(description)
        if requirements:
            requirements = requirements[:3000]

        employment_type = _clean_text(raw_job.get("employmentType"))
        seniority_hint = _clean_text(raw_job.get("experienceLevel")) or _clean_text(
            raw_job.get("seniority")
        )
        seniority_hint = _infer_seniority_hint(title, description, seniority_hint)

        remote_policy = _clean_text(raw_job.get("locationType"))
        if remote_policy is None:
            if raw_job.get("isRemote") is True:
                remote_policy = "REMOTE"
            elif location and "remote" in location.lower():
                remote_policy = "REMOTE"

        tech_stack = _extract_tech_stack(description, requirements, title)

        jobs.append(
            {
                "title": title,
                "company": organization,
                "location": location,
                "description": description,
                "requirements": requirements,
                "employment_type": employment_type,
                "seniority_hint": seniority_hint,
                "remote_policy": remote_policy,
                "tech_stack": tech_stack,
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
