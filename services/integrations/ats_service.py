import asyncio
import html
import hashlib
import re
from datetime import date, datetime, timezone
from typing import Any, Awaitable, Callable
from urllib.parse import urljoin

import httpx

from core.config.config import (
    ATS_ASHBY_ORGANIZATIONS,
    ATS_BREEZY_ORGANIZATIONS,
    ATS_ENABLE_MOCK_FALLBACK,
    ATS_ENABLE_UNOFFICIAL_SOURCES,
    ATS_GREENHOUSE_BOARDS,
    ATS_GUPY_API_TOKEN,
    ATS_GUPY_COMPANIES,
    ATS_LEVER_COMPANIES,
    ATS_MAX_JOBS_PER_SOURCE,
    ATS_RECRUITEE_COMPANIES,
    ATS_SMARTRECRUITERS_COMPANIES,
    ATS_WORKABLE_API_TOKEN,
    ATS_WORKABLE_COMPANIES,
    ATS_WORKDAY_COMPANIES,
)
from core.http.http_client import http_client
from core.logger.logger import logger


def _parse_handles(raw_value: str) -> list[str]:
    handles: list[str] = []
    seen: set[str] = set()

    for item in re.split(r"[,;\n\r]+", raw_value):
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


def _parse_source_posted_at(value: Any) -> str | None:
    if isinstance(value, datetime):
        parsed_value = value
        if parsed_value.tzinfo is None:
            parsed_value = parsed_value.replace(tzinfo=timezone.utc)
        return parsed_value.isoformat()

    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000.0

        try:
            parsed_value = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            return parsed_value.isoformat()
        except (OverflowError, OSError, ValueError):
            return None

    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None

    numeric_value = normalized.replace(".", "", 1)
    if numeric_value.isdigit():
        return _parse_source_posted_at(float(normalized))

    try:
        parsed_value = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed_value.tzinfo is None:
        parsed_value = parsed_value.replace(tzinfo=timezone.utc)

    return parsed_value.isoformat()



def _coerce_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        coerced = value
        if coerced.tzinfo is None:
            coerced = coerced.replace(tzinfo=timezone.utc)
        return coerced.date()

    parsed_iso = _parse_source_posted_at(value)
    if not parsed_iso:
        return None

    try:
        return datetime.fromisoformat(parsed_iso.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _coerce_query_datetime_iso(value: Any) -> str | None:
    parsed_date = _coerce_date(value)
    if parsed_date is None:
        return None

    return f"{parsed_date.isoformat()}T00:00:00Z"


def _apply_date_window(
    jobs: list[dict],
    date_from_value: Any = None,
    date_to_value: Any = None,
) -> tuple[list[dict], dict | None]:
    date_from = _coerce_date(date_from_value)
    date_to = _coerce_date(date_to_value)

    if date_from is None and date_to is None:
        return jobs, None

    if date_from is None or date_to is None:
        return jobs, None

    if date_from > date_to:
        date_from, date_to = date_to, date_from

    filtered_jobs: list[dict] = []
    ignored_without_date = 0

    for job in jobs:
        posted_date = _coerce_date(job.get("source_posted_at"))

        if posted_date is None:
            filtered_jobs.append(job)
            ignored_without_date += 1
            continue

        if date_from <= posted_date <= date_to:
            filtered_jobs.append(job)

    return filtered_jobs, {
        "dateFrom": date_from.isoformat(),
        "dateTo": date_to.isoformat(),
        "inputCount": len(jobs),
        "outputCount": len(filtered_jobs),
        "jobsWithoutSourceDate": ignored_without_date,
        "filteredOutByDate": max(0, len(jobs) - len(filtered_jobs)),
    }


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


def _absolute_url(base_url: str, raw_url: str | None) -> str | None:
    url_text = _clean_text(raw_url)
    if not url_text:
        return None

    try:
        resolved = urljoin(base_url, url_text)
    except Exception:
        return None

    resolved = resolved.strip()
    if not resolved.startswith("http"):
        return None

    return resolved


def _extract_job_links_from_html(
    page_html: str,
    base_url: str,
    allowed_link_tokens: tuple[str, ...],
    max_jobs: int,
) -> list[dict]:
    link_pattern = re.compile(
        r"<a\s+[^>]*href=[\"'](?P<href>[^\"']+)[\"'][^>]*>(?P<label>.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )

    links: list[dict] = []
    seen_urls: set[str] = set()

    for match in link_pattern.finditer(page_html):
        href = _clean_text(match.group("href"))
        if not href:
            continue

        href_lower = href.lower()
        if href_lower.startswith("mailto:") or href_lower.startswith("tel:"):
            continue

        if not any(token in href_lower for token in allowed_link_tokens):
            continue

        absolute = _absolute_url(base_url, href)
        if not absolute:
            continue

        dedupe_url = absolute.lower()
        if dedupe_url in seen_urls:
            continue

        label = _plain_text(match.group("label"))
        if not label:
            continue

        if len(label) > 180:
            label = label[:180].strip()

        links.append(
            {
                "title": label,
                "url": absolute,
            }
        )
        seen_urls.add(dedupe_url)

        if len(links) >= max_jobs:
            break

    return links


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
    source_posted_at = datetime.now(timezone.utc).isoformat()

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
            "source_target": "mock",
            "source_url": "https://jobs.example.com/backend-engineer",
            "external_job_id": "wa-backend-engineer",
            "source_posted_at": source_posted_at,
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
            "source_target": "mock",
            "source_url": "https://jobs.example.com/python-developer",
            "external_job_id": "wa-python-developer",
            "source_posted_at": source_posted_at,
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
            "source_target": "mock",
            "source_url": "https://jobs.example.com/data-engineer",
            "external_job_id": "wa-data-engineer",
            "source_posted_at": source_posted_at,
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
                "source_target": "mock",
                "source_url": "https://jobs.example.com/sre",
                "external_job_id": "wa-sre-engineer",
                "source_posted_at": source_posted_at,
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
        source_posted_at = _parse_source_posted_at(
            raw_job.get("updated_at")
            or raw_job.get("first_published")
            or raw_job.get("created_at")
        )
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
                "source_target": board_token,
                "source_url": source_url,
                "external_job_id": external_job_id,
                "source_posted_at": source_posted_at,
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
        source_posted_at = _parse_source_posted_at(
            raw_job.get("createdAt") or raw_job.get("updatedAt")
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
                "source_target": company_handle,
                "source_url": source_url,
                "external_job_id": external_job_id,
                "source_posted_at": source_posted_at,
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
        source_posted_at = _parse_source_posted_at(
            raw_job.get("publishedAt")
            or raw_job.get("createdAt")
            or raw_job.get("updatedAt")
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
                "source_target": organization,
                "source_url": source_url,
                "external_job_id": external_job_id,
                "source_posted_at": source_posted_at,
            }
        )

    return jobs


async def _fetch_breezy_jobs(
    organization: str,
    client: httpx.AsyncClient,
    max_jobs: int,
) -> list[dict]:
    response = await client.get(
        f"https://{organization}.breezy.hr/json",
        follow_redirects=True,
    )
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list):
        return []

    jobs: list[dict] = []
    for raw_job in payload[:max_jobs]:
        if not isinstance(raw_job, dict):
            continue

        title = _clean_text(raw_job.get("name"))
        if not title:
            continue

        location = _clean_text(raw_job.get("location"))
        locations = raw_job.get("locations")
        if not location and isinstance(locations, list) and locations:
            location = _clean_text(locations[0])

        source_url = _clean_text(raw_job.get("url"))
        source_posted_at = _parse_source_posted_at(raw_job.get("published_date"))
        company = (
            _clean_text((raw_job.get("company") or {}).get("name"))
            if isinstance(raw_job.get("company"), dict)
            else None
        ) or organization

        employment_type = _clean_text(raw_job.get("type"))
        seniority_hint = _infer_seniority_hint(title, None)

        location_text = (location or "").lower()
        remote_policy = None
        if "remote" in location_text or "remoto" in location_text:
            remote_policy = "REMOTE"
        elif "hybrid" in location_text or "hibrido" in location_text:
            remote_policy = "HYBRID"

        external_job_id = _stable_external_job_id(
            "breezy",
            organization,
            raw_job.get("id") or raw_job.get("friendly_id"),
            source_url,
            title,
        )

        jobs.append(
            {
                "title": title,
                "company": company,
                "location": location,
                "description": None,
                "requirements": None,
                "employment_type": employment_type,
                "seniority_hint": seniority_hint,
                "remote_policy": remote_policy,
                "tech_stack": _extract_tech_stack(title),
                "source": "breezy",
                "source_target": organization,
                "source_url": source_url,
                "external_job_id": external_job_id,
                "source_posted_at": source_posted_at,
            }
        )

    return jobs


async def _fetch_smartrecruiters_jobs(
    company: str,
    client: httpx.AsyncClient,
    max_jobs: int,
    released_after: str | None = None,
) -> list[dict]:
    jobs: list[dict] = []
    offset = 0
    per_page = max(1, min(100, max_jobs))

    while len(jobs) < max_jobs:
        request_params: dict[str, Any] = {
            "offset": offset,
            "limit": per_page,
        }
        if released_after:
            request_params["releasedAfter"] = released_after

        response = await client.get(
            f"https://api.smartrecruiters.com/v1/companies/{company}/postings",
            params=request_params,
        )
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, dict):
            break

        raw_jobs = payload.get("content")
        if not isinstance(raw_jobs, list) or not raw_jobs:
            break

        for raw_job in raw_jobs:
            if not isinstance(raw_job, dict):
                continue

            title = _clean_text(raw_job.get("name"))
            if not title:
                continue

            location_payload = raw_job.get("location")
            location = None
            remote_policy = None
            if isinstance(location_payload, dict):
                location = _clean_text(location_payload.get("fullLocation")) or _clean_text(
                    ", ".join(
                        [
                            str(part).strip()
                            for part in [
                                location_payload.get("city"),
                                location_payload.get("region"),
                                location_payload.get("country"),
                            ]
                            if str(part).strip()
                        ]
                    )
                )
                if location_payload.get("remote") is True:
                    remote_policy = "REMOTE"
                elif location_payload.get("hybrid") is True:
                    remote_policy = "HYBRID"

            source_url = _clean_text(raw_job.get("ref"))
            source_posted_at = _parse_source_posted_at(raw_job.get("releasedDate"))

            company_payload = raw_job.get("company")
            company_name = (
                _clean_text(company_payload.get("name"))
                if isinstance(company_payload, dict)
                else None
            ) or company

            experience_payload = raw_job.get("experienceLevel")
            experience_label = (
                _clean_text(experience_payload.get("label"))
                if isinstance(experience_payload, dict)
                else None
            )
            seniority_hint = _infer_seniority_hint(title, None, experience_label)

            employment_payload = raw_job.get("typeOfEmployment")
            employment_type = (
                _clean_text(employment_payload.get("label"))
                if isinstance(employment_payload, dict)
                else None
            )

            custom_fields = raw_job.get("customField")
            requirement_chunks: list[str] = []
            if isinstance(custom_fields, list):
                for custom_field in custom_fields:
                    if not isinstance(custom_field, dict):
                        continue

                    field_label = (_clean_text(custom_field.get("fieldLabel")) or "").lower()
                    value_label = _clean_text(custom_field.get("valueLabel"))
                    if not field_label or not value_label:
                        continue

                    if (
                        "require" in field_label
                        or "skill" in field_label
                        or "qualification" in field_label
                        or "stack" in field_label
                    ):
                        requirement_chunks.append(value_label)

            requirements = _clean_text("; ".join(requirement_chunks))
            description = None
            if requirements:
                description = f"Department: {(_clean_text((raw_job.get('department') or {}).get('label')) if isinstance(raw_job.get('department'), dict) else '') or 'N/A'}"

            external_job_id = _stable_external_job_id(
                "smartrecruiters",
                company,
                raw_job.get("id") or raw_job.get("uuid"),
                source_url,
                title,
            )

            jobs.append(
                {
                    "title": title,
                    "company": company_name,
                    "location": location,
                    "description": description,
                    "requirements": requirements,
                    "employment_type": employment_type,
                    "seniority_hint": seniority_hint,
                    "remote_policy": remote_policy,
                    "tech_stack": _extract_tech_stack(title, requirements),
                    "source": "smartrecruiters",
                    "source_target": company,
                    "source_url": source_url,
                    "external_job_id": external_job_id,
                    "source_posted_at": source_posted_at,
                }
            )

            if len(jobs) >= max_jobs:
                break

        if len(raw_jobs) < per_page:
            break

        offset += len(raw_jobs)

    return jobs[:max_jobs]


async def _fetch_recruitee_jobs(
    company: str,
    client: httpx.AsyncClient,
    max_jobs: int,
) -> list[dict]:
    response = await client.get(
        f"https://{company}.recruitee.com/api/offers/",
        follow_redirects=True,
    )
    response.raise_for_status()

    payload = response.json()
    raw_jobs = payload.get("offers") if isinstance(payload, dict) else []
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
        if not location:
            location = _clean_text(
                ", ".join(
                    [
                        str(part).strip()
                        for part in [
                            raw_job.get("city"),
                            raw_job.get("state_name"),
                            raw_job.get("country"),
                        ]
                        if str(part).strip()
                    ]
                )
            )

        source_url = _clean_text(raw_job.get("careers_url")) or _clean_text(
            raw_job.get("careers_apply_url")
        )
        source_posted_at = _parse_source_posted_at(
            raw_job.get("published_at")
            or raw_job.get("created_at")
            or raw_job.get("updated_at")
        )

        description = _plain_text(raw_job.get("description"))
        requirements = _plain_text(raw_job.get("requirements"))
        if requirements is None:
            requirements = _extract_requirements_from_text(description)
        if requirements:
            requirements = requirements[:3000]

        seniority_hint = _infer_seniority_hint(
            title,
            description,
            _clean_text(raw_job.get("experience_code"))
            or _clean_text(raw_job.get("experience_level")),
        )

        remote_policy = None
        if raw_job.get("remote") is True:
            remote_policy = "REMOTE"
        elif raw_job.get("hybrid") is True:
            remote_policy = "HYBRID"
        elif raw_job.get("on_site") is True:
            remote_policy = "ONSITE"

        employment_type = _clean_text(raw_job.get("employment_type_code")) or _clean_text(
            raw_job.get("employment_type")
        )

        external_job_id = _stable_external_job_id(
            "recruitee",
            company,
            raw_job.get("id") or raw_job.get("guid") or raw_job.get("slug"),
            source_url,
            title,
        )

        jobs.append(
            {
                "title": title,
                "company": _clean_text(raw_job.get("company_name")) or company,
                "location": location,
                "description": description,
                "requirements": requirements,
                "employment_type": employment_type,
                "seniority_hint": seniority_hint,
                "remote_policy": remote_policy,
                "tech_stack": _extract_tech_stack(description, requirements, title),
                "source": "recruitee",
                "source_target": company,
                "source_url": source_url,
                "external_job_id": external_job_id,
                "source_posted_at": source_posted_at,
            }
        )

    return jobs


async def _fetch_workable_jobs(
    company: str,
    client: httpx.AsyncClient,
    max_jobs: int,
    updated_after: str | None = None,
    created_after: str | None = None,
) -> list[dict]:
    token = _clean_text(ATS_WORKABLE_API_TOKEN)
    jobs: list[dict] = []

    if token:
        per_page = max(1, min(100, max_jobs))
        request_headers = {"Authorization": f"Bearer {token}"}
        request_url: str | None = f"https://{company}.workable.com/spi/v3/jobs"
        request_params: dict[str, Any] | None = {
            "state": "published",
            "limit": per_page,
            "include_fields": "description,requirements,employment_type,location,created_at,updated_at",
        }

        if updated_after:
            request_params["updated_after"] = updated_after
        if created_after:
            request_params["created_after"] = created_after

        try:
            while request_url and len(jobs) < max_jobs:
                response = await client.get(
                    request_url,
                    headers=request_headers,
                    params=request_params,
                    follow_redirects=True,
                )
                response.raise_for_status()

                payload = response.json()
                raw_jobs = (
                    payload.get("results")
                    or payload.get("jobs")
                    or payload.get("data")
                    or []
                )
                if not isinstance(raw_jobs, list) or not raw_jobs:
                    break

                for raw_job in raw_jobs:
                    if not isinstance(raw_job, dict):
                        continue

                    title = _clean_text(raw_job.get("title") or raw_job.get("name"))
                    if not title:
                        continue

                    source_url = _clean_text(
                        raw_job.get("url")
                        or raw_job.get("application_url")
                        or raw_job.get("shortlink")
                    )

                    shortcode = _clean_text(raw_job.get("shortcode") or raw_job.get("code"))
                    if source_url is None and shortcode:
                        source_url = f"https://apply.workable.com/{company}/j/{shortcode}/"

                    location_payload = raw_job.get("location")
                    location = None
                    if isinstance(location_payload, dict):
                        location = _clean_text(
                            location_payload.get("location_str")
                            or location_payload.get("city")
                            or ", ".join(
                                [
                                    str(part).strip()
                                    for part in [
                                        location_payload.get("city"),
                                        location_payload.get("region"),
                                        location_payload.get("country"),
                                    ]
                                    if str(part).strip()
                                ]
                            )
                        )
                    elif isinstance(location_payload, str):
                        location = _clean_text(location_payload)

                    description = _plain_text(
                        raw_job.get("description")
                        or raw_job.get("description_plain")
                    )
                    requirements = _plain_text(raw_job.get("requirements"))
                    if requirements is None:
                        requirements = _extract_requirements_from_text(description)
                    if requirements:
                        requirements = requirements[:3000]

                    employment_type_payload = raw_job.get("employment_type")
                    employment_type = (
                        _clean_text(employment_type_payload.get("name"))
                        if isinstance(employment_type_payload, dict)
                        else _clean_text(employment_type_payload)
                    )

                    remote_policy = None
                    if raw_job.get("remote") is True:
                        remote_policy = "REMOTE"
                    elif raw_job.get("hybrid") is True:
                        remote_policy = "HYBRID"
                    elif location and "remote" in location.lower():
                        remote_policy = "REMOTE"

                    seniority_hint = _infer_seniority_hint(
                        title,
                        description,
                        _clean_text(raw_job.get("seniority"))
                        or _clean_text(raw_job.get("experience")),
                    )

                    source_posted_at = _parse_source_posted_at(
                        raw_job.get("updated_at")
                        or raw_job.get("created_at")
                        or raw_job.get("published_at")
                    )

                    external_job_id = _stable_external_job_id(
                        "workable",
                        company,
                        raw_job.get("id") or shortcode,
                        source_url,
                        title,
                    )

                    company_name = (
                        _clean_text((raw_job.get("account") or {}).get("name"))
                        if isinstance(raw_job.get("account"), dict)
                        else None
                    ) or company

                    jobs.append(
                        {
                            "title": title,
                            "company": company_name,
                            "location": location,
                            "description": description,
                            "requirements": requirements,
                            "employment_type": employment_type,
                            "seniority_hint": seniority_hint,
                            "remote_policy": remote_policy,
                            "tech_stack": _extract_tech_stack(
                                title,
                                description,
                                requirements,
                            ),
                            "source": "workable",
                            "source_target": company,
                            "source_url": source_url,
                            "external_job_id": external_job_id,
                            "source_posted_at": source_posted_at,
                        }
                    )

                    if len(jobs) >= max_jobs:
                        return jobs[:max_jobs]

                paging_payload = payload.get("paging") if isinstance(payload, dict) else None
                next_url = (
                    _clean_text(paging_payload.get("next"))
                    if isinstance(paging_payload, dict)
                    else None
                )
                request_url = _absolute_url(str(response.url), next_url) if next_url else None
                request_params = None
        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code if error.response else None
            logger.warning(
                "workable_official_http_error target=%s status=%s",
                company,
                status_code,
            )
            if not ATS_ENABLE_UNOFFICIAL_SOURCES:
                raise
        except httpx.HTTPError as error:
            logger.warning(
                "workable_official_transport_error target=%s error=%s",
                company,
                error,
            )
            if not ATS_ENABLE_UNOFFICIAL_SOURCES:
                raise

        if jobs:
            return jobs[:max_jobs]

    if not ATS_ENABLE_UNOFFICIAL_SOURCES:
        if token is None:
            logger.warning(
                "workable_official_missing_token target=%s",
                company,
            )
        return []

    base_url = f"https://apply.workable.com/{company}/"
    response = await client.get(base_url, follow_redirects=True)
    response.raise_for_status()

    link_rows = _extract_job_links_from_html(
        response.text,
        str(response.url),
        ("/j/", "/jobs/", f"/{company}/"),
        max_jobs,
    )

    jobs: list[dict] = []
    for row in link_rows:
        title = _clean_text(row.get("title"))
        source_url = _clean_text(row.get("url"))
        if not title or not source_url:
            continue

        external_job_id = _stable_external_job_id(
            "workable",
            company,
            None,
            source_url,
            title,
        )

        jobs.append(
            {
                "title": title,
                "company": company,
                "location": None,
                "description": None,
                "requirements": None,
                "employment_type": None,
                "seniority_hint": _infer_seniority_hint(title, None),
                "remote_policy": None,
                "tech_stack": _extract_tech_stack(title),
                "source": "workable",
                "source_target": company,
                "source_url": source_url,
                "external_job_id": external_job_id,
                "source_posted_at": None,
            }
        )

    return jobs


async def _fetch_gupy_jobs(
    company: str,
    client: httpx.AsyncClient,
    max_jobs: int,
    updated_after: str | None = None,
) -> list[dict]:
    token = _clean_text(ATS_GUPY_API_TOKEN)
    jobs: list[dict] = []

    if token:
        request_headers = {"Authorization": f"Bearer {token}"}
        per_page = max(1, min(100, max_jobs))
        next_page_token: str | None = None

        try:
            while len(jobs) < max_jobs:
                request_params: dict[str, Any] = {
                    "status": "published",
                    "limit": per_page,
                    "careerPageIds": company,
                }
                if updated_after:
                    request_params["updatedAfter"] = updated_after
                if next_page_token:
                    request_params["pageToken"] = next_page_token

                response = await client.get(
                    "https://api.gupy.io/api/v2/jobs",
                    headers=request_headers,
                    params=request_params,
                )
                response.raise_for_status()

                payload = response.json()
                raw_jobs = (
                    payload.get("jobs")
                    or payload.get("results")
                    or payload.get("data")
                    or payload.get("content")
                    or []
                )
                if not isinstance(raw_jobs, list) or not raw_jobs:
                    break

                for raw_job in raw_jobs:
                    if not isinstance(raw_job, dict):
                        continue

                    title = _clean_text(raw_job.get("title") or raw_job.get("name"))
                    if not title:
                        continue

                    source_url = _clean_text(
                        raw_job.get("jobUrl")
                        or raw_job.get("url")
                        or raw_job.get("publicUrl")
                        or raw_job.get("externalUrl")
                    )

                    if source_url and not source_url.startswith("http"):
                        source_url = _absolute_url(f"https://{company}.gupy.io/", source_url)

                    location = None
                    location_payload = raw_job.get("location")
                    if isinstance(location_payload, dict):
                        location = _clean_text(
                            location_payload.get("name")
                            or ", ".join(
                                [
                                    str(part).strip()
                                    for part in [
                                        location_payload.get("city"),
                                        location_payload.get("state"),
                                        location_payload.get("country"),
                                    ]
                                    if str(part).strip()
                                ]
                            )
                        )
                    else:
                        location = _clean_text(location_payload)

                    description = _plain_text(raw_job.get("description"))
                    requirements = _plain_text(raw_job.get("requirements"))
                    if requirements is None:
                        requirements = _extract_requirements_from_text(description)
                    if requirements:
                        requirements = requirements[:3000]

                    employment_type = _clean_text(
                        raw_job.get("employmentType")
                        or raw_job.get("contractType")
                    )

                    remote_policy = None
                    if raw_job.get("remote") is True:
                        remote_policy = "REMOTE"
                    elif raw_job.get("hybrid") is True:
                        remote_policy = "HYBRID"
                    elif raw_job.get("onSite") is True:
                        remote_policy = "ONSITE"

                    if remote_policy is None:
                        work_model = (_clean_text(raw_job.get("workModel")) or "").lower()
                        if "remote" in work_model or "remoto" in work_model:
                            remote_policy = "REMOTE"
                        elif "hybrid" in work_model or "hibrido" in work_model:
                            remote_policy = "HYBRID"

                    source_posted_at = _parse_source_posted_at(
                        raw_job.get("updatedAt")
                        or raw_job.get("publishedAt")
                        or raw_job.get("createdAt")
                    )

                    seniority_hint = _infer_seniority_hint(
                        title,
                        description,
                        _clean_text(raw_job.get("seniority"))
                        or _clean_text(raw_job.get("experienceLevel")),
                    )

                    external_job_id = _stable_external_job_id(
                        "gupy",
                        company,
                        raw_job.get("id") or raw_job.get("code"),
                        source_url,
                        title,
                    )

                    company_name = _clean_text(
                        raw_job.get("companyName")
                        or ((raw_job.get("company") or {}).get("name") if isinstance(raw_job.get("company"), dict) else None)
                    ) or company

                    jobs.append(
                        {
                            "title": title,
                            "company": company_name,
                            "location": location,
                            "description": description,
                            "requirements": requirements,
                            "employment_type": employment_type,
                            "seniority_hint": seniority_hint,
                            "remote_policy": remote_policy,
                            "tech_stack": _extract_tech_stack(
                                title,
                                description,
                                requirements,
                            ),
                            "source": "gupy",
                            "source_target": company,
                            "source_url": source_url,
                            "external_job_id": external_job_id,
                            "source_posted_at": source_posted_at,
                        }
                    )

                    if len(jobs) >= max_jobs:
                        return jobs[:max_jobs]

                next_page_token = _clean_text(
                    payload.get("nextPageToken")
                    or payload.get("next_page_token")
                    or (
                        (payload.get("pagination") or {}).get("nextPageToken")
                        if isinstance(payload.get("pagination"), dict)
                        else None
                    )
                )
                if not next_page_token:
                    break
        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code if error.response else None
            logger.warning(
                "gupy_official_http_error target=%s status=%s",
                company,
                status_code,
            )
            if not ATS_ENABLE_UNOFFICIAL_SOURCES:
                raise
        except httpx.HTTPError as error:
            logger.warning(
                "gupy_official_transport_error target=%s error=%s",
                company,
                error,
            )
            if not ATS_ENABLE_UNOFFICIAL_SOURCES:
                raise

        if jobs:
            return jobs[:max_jobs]

    if not ATS_ENABLE_UNOFFICIAL_SOURCES:
        if token is None:
            logger.warning("gupy_official_missing_token target=%s", company)
        return []

    base_url = f"https://{company}.gupy.io/"
    response = await client.get(base_url, follow_redirects=True)
    response.raise_for_status()

    link_rows = _extract_job_links_from_html(
        response.text,
        str(response.url),
        ("/jobs/", "/job/", "/vagas/"),
        max_jobs,
    )

    jobs = []
    for row in link_rows:
        title = _clean_text(row.get("title"))
        source_url = _clean_text(row.get("url"))
        if not title or not source_url:
            continue

        external_job_id = _stable_external_job_id(
            "gupy",
            company,
            None,
            source_url,
            title,
        )

        jobs.append(
            {
                "title": title,
                "company": company,
                "location": None,
                "description": None,
                "requirements": None,
                "employment_type": None,
                "seniority_hint": _infer_seniority_hint(title, None),
                "remote_policy": None,
                "tech_stack": _extract_tech_stack(title),
                "source": "gupy",
                "source_target": company,
                "source_url": source_url,
                "external_job_id": external_job_id,
                "source_posted_at": None,
            }
        )

    return jobs


async def _fetch_workday_jobs(
    company: str,
    client: httpx.AsyncClient,
    max_jobs: int,
) -> list[dict]:
    site_candidates = (
        "External",
        "External_Career_Site",
        "Careers",
        "CareerSite",
        "Careers_External",
    )

    jobs: list[dict] = []
    for site_name in site_candidates:
        response = await client.get(
            f"https://{company}.wd3.myworkdayjobs.com/wday/cxs/{company}/{site_name}/jobs",
            params={"limit": max(1, min(100, max_jobs)), "offset": 0},
            follow_redirects=True,
        )

        if response.status_code != 200:
            continue

        if "application/json" not in (response.headers.get("content-type") or ""):
            continue

        payload = response.json()
        if not isinstance(payload, dict):
            continue

        raw_items = (
            payload.get("jobPostings")
            or payload.get("jobs")
            or payload.get("content")
            or []
        )
        if not isinstance(raw_items, list) or not raw_items:
            continue

        for raw_item in raw_items[:max_jobs]:
            if not isinstance(raw_item, dict):
                continue

            title = _clean_text(raw_item.get("title") or raw_item.get("name"))
            if not title:
                continue

            source_url = _clean_text(raw_item.get("externalPath"))
            if source_url and not source_url.startswith("http"):
                source_url = _absolute_url(
                    f"https://{company}.wd3.myworkdayjobs.com/en-US/{site_name}/",
                    source_url,
                )

            location = _clean_text(
                raw_item.get("locationsText")
                or raw_item.get("location")
                or raw_item.get("locations")
            )

            description = _plain_text(raw_item.get("description"))
            requirements = _extract_requirements_from_text(description)

            external_job_id = _stable_external_job_id(
                "workday",
                company,
                raw_item.get("bulletFields") or raw_item.get("id"),
                source_url,
                title,
            )

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "description": description,
                    "requirements": requirements,
                    "employment_type": None,
                    "seniority_hint": _infer_seniority_hint(title, description),
                    "remote_policy": None,
                    "tech_stack": _extract_tech_stack(title, description, requirements),
                    "source": "workday",
                    "source_target": company,
                    "source_url": source_url,
                    "external_job_id": external_job_id,
                    "source_posted_at": _parse_source_posted_at(
                        raw_item.get("postedOn")
                        or raw_item.get("postedDate")
                        or raw_item.get("publishedAt")
                    ),
                }
            )

            if len(jobs) >= max_jobs:
                return jobs[:max_jobs]

        if jobs:
            return jobs[:max_jobs]

    return jobs[:max_jobs]


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


async def fetch_jobs(
    force: bool = False,
    date_from: Any = None,
    date_to: Any = None,
) -> dict:
    greenhouse_boards = _parse_handles(ATS_GREENHOUSE_BOARDS)
    lever_companies = _parse_handles(ATS_LEVER_COMPANIES)
    ashby_organizations = _parse_handles(ATS_ASHBY_ORGANIZATIONS)
    workable_companies = _parse_handles(ATS_WORKABLE_COMPANIES)
    breezy_organizations = _parse_handles(ATS_BREEZY_ORGANIZATIONS)
    smartrecruiters_companies = _parse_handles(ATS_SMARTRECRUITERS_COMPANIES)
    recruitee_companies = _parse_handles(ATS_RECRUITEE_COMPANIES)
    gupy_companies = _parse_handles(ATS_GUPY_COMPANIES)
    workday_companies = _parse_handles(ATS_WORKDAY_COMPANIES)

    window_start_iso = _coerce_query_datetime_iso(date_from)
    workable_token_available = bool(_clean_text(ATS_WORKABLE_API_TOKEN))
    gupy_token_available = bool(_clean_text(ATS_GUPY_API_TOKEN))

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
    configured_targets.extend(
        (
            "smartrecruiters",
            company,
            lambda target, client, max_jobs, released_after=window_start_iso: _fetch_smartrecruiters_jobs(
                target,
                client,
                max_jobs,
                released_after=released_after,
            ),
        )
        for company in smartrecruiters_companies
    )
    configured_targets.extend(
        ("recruitee", company, _fetch_recruitee_jobs)
        for company in recruitee_companies
    )

    if workable_token_available or ATS_ENABLE_UNOFFICIAL_SOURCES:
        configured_targets.extend(
            (
                "workable",
                company,
                lambda target, client, max_jobs, updated_after=window_start_iso: _fetch_workable_jobs(
                    target,
                    client,
                    max_jobs,
                    updated_after=updated_after,
                    created_after=updated_after,
                ),
            )
            for company in workable_companies
        )
    elif workable_companies:
        logger.warning(
            "workable_sources_skipped_missing_token count=%s",
            len(workable_companies),
        )

    if gupy_token_available or ATS_ENABLE_UNOFFICIAL_SOURCES:
        configured_targets.extend(
            (
                "gupy",
                company,
                lambda target, client, max_jobs, updated_after=window_start_iso: _fetch_gupy_jobs(
                    target,
                    client,
                    max_jobs,
                    updated_after=updated_after,
                ),
            )
            for company in gupy_companies
        )
    elif gupy_companies:
        logger.warning(
            "gupy_sources_skipped_missing_token count=%s",
            len(gupy_companies),
        )

    if ATS_ENABLE_UNOFFICIAL_SOURCES:
        configured_targets.extend(
            ("breezy", organization, _fetch_breezy_jobs)
            for organization in breezy_organizations
        )
        configured_targets.extend(
            ("workday", company, _fetch_workday_jobs) for company in workday_companies
        )

    if not configured_targets:
        if ATS_ENABLE_MOCK_FALLBACK:
            jobs = _build_mock_jobs(force)
            jobs, window_context = _apply_date_window(jobs, date_from, date_to)
            return {
                "status": True,
                "message": "No ATS source configured. Using mock ingestion jobs",
                "data": {
                    "jobs": jobs,
                    "sources": [],
                    "fallbackUsed": True,
                    "configuredSources": 0,
                    "successfulSources": 0,
                    "window": window_context,
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

        jobs, window_context = _apply_date_window(jobs, date_from, date_to)

        if not jobs:
            return {
                "status": False,
                "message": "ATS fetch finished with no jobs",
                "data": {"window": window_context},
            }

        successful_sources = sum(
            1 for source in source_metadata if source["status"] == "ok"
        )

        message = f"Fetched {len(jobs)} jobs from ATS sources"
        if fallback_used:
            message = f"{message}; mock fallback enabled"

        if window_context:
            message = (
                f"{message}; window {window_context['dateFrom']}..{window_context['dateTo']}"
            )

        return {
            "status": True,
            "message": message,
            "data": {
                "jobs": jobs,
                "sources": source_metadata,
                "fallbackUsed": fallback_used,
                "configuredSources": len(configured_targets),
                "successfulSources": successful_sources,
                "window": window_context,
            },
        }
    except Exception as error:
        logger.exception(error)

        if ATS_ENABLE_MOCK_FALLBACK:
            jobs = _build_mock_jobs(force)
            jobs, window_context = _apply_date_window(jobs, date_from, date_to)
            return {
                "status": True,
                "message": "ATS fetch failed. Using mock ingestion jobs",
                "data": {
                    "jobs": jobs,
                    "sources": [],
                    "fallbackUsed": True,
                    "configuredSources": len(configured_targets),
                    "successfulSources": 0,
                    "window": window_context,
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
    workable_companies = _parse_handles(ATS_WORKABLE_COMPANIES)
    breezy_organizations = _parse_handles(ATS_BREEZY_ORGANIZATIONS)
    smartrecruiters_companies = _parse_handles(ATS_SMARTRECRUITERS_COMPANIES)
    recruitee_companies = _parse_handles(ATS_RECRUITEE_COMPANIES)
    gupy_companies = _parse_handles(ATS_GUPY_COMPANIES)
    workday_companies = _parse_handles(ATS_WORKDAY_COMPANIES)
    workable_token_available = bool(_clean_text(ATS_WORKABLE_API_TOKEN))
    gupy_token_available = bool(_clean_text(ATS_GUPY_API_TOKEN))

    return {
        "status": True,
        "message": "ATS integration ready",
        "data": {
            "module": "ats",
            "sourcesConfigured": {
                "greenhouse": len(greenhouse_boards),
                "lever": len(lever_companies),
                "ashby": len(ashby_organizations),
                "workable": len(workable_companies),
                "breezy": len(breezy_organizations),
                "smartrecruiters": len(smartrecruiters_companies),
                "recruitee": len(recruitee_companies),
                "gupy": len(gupy_companies),
                "workday": len(workday_companies),
            },
            "sourcesActive": {
                "greenhouse": len(greenhouse_boards),
                "lever": len(lever_companies),
                "ashby": len(ashby_organizations),
                "workable": len(workable_companies)
                if (workable_token_available or ATS_ENABLE_UNOFFICIAL_SOURCES)
                else 0,
                "breezy": len(breezy_organizations) if ATS_ENABLE_UNOFFICIAL_SOURCES else 0,
                "smartrecruiters": len(smartrecruiters_companies),
                "recruitee": len(recruitee_companies),
                "gupy": len(gupy_companies)
                if (gupy_token_available or ATS_ENABLE_UNOFFICIAL_SOURCES)
                else 0,
                "workday": len(workday_companies) if ATS_ENABLE_UNOFFICIAL_SOURCES else 0,
            },
            "auth": {
                "workableTokenConfigured": workable_token_available,
                "gupyTokenConfigured": gupy_token_available,
            },
            "unofficialSourcesEnabled": ATS_ENABLE_UNOFFICIAL_SOURCES,
            "mockFallbackEnabled": ATS_ENABLE_MOCK_FALLBACK,
            "maxJobsPerSource": ATS_MAX_JOBS_PER_SOURCE,
        },
    }

