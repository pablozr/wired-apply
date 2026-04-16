import json
from typing import TypedDict

import asyncpg
from pydantic import BaseModel, Field, field_validator


class JobCreateRequest(BaseModel):
    title: str = Field(min_length=1)
    company: str = Field(min_length=1)
    location: str | None = None
    description: str | None = None
    requirements: str | None = None
    employment_type: str | None = Field(default=None, alias="employmentType")
    seniority_hint: str | None = Field(default=None, alias="seniorityHint")
    remote_policy: str | None = Field(default=None, alias="remotePolicy")
    tech_stack: list[str] = Field(default_factory=list, alias="techStack")
    source: str = Field(default="manual", min_length=1)
    source_url: str | None = Field(default=None, alias="sourceUrl")
    external_job_id: str | None = Field(default=None, alias="externalJobId")
    status: str = Field(default="NEW", min_length=1)

    model_config = {"populate_by_name": True}

    @field_validator(
        "title",
        "company",
        "location",
        "description",
        "requirements",
        "employment_type",
        "seniority_hint",
        "remote_policy",
        "source_url",
        "external_job_id",
    )
    @classmethod
    def normalize_text(cls, value: str | None) -> str | None:
        if value is None:
            return None

        normalized = value.strip()

        return normalized or None

    @field_validator("source")
    @classmethod
    def normalize_source(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("status")
    @classmethod
    def normalize_status(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("tech_stack", mode="before")
    @classmethod
    def normalize_tech_stack(cls, value: list[str] | None) -> list[str]:
        if not isinstance(value, list):
            return []

        normalized: list[str] = []
        seen: set[str] = set()
        for item in value:
            token = str(item).strip()
            if not token:
                continue

            dedupe_key = token.lower()
            if dedupe_key in seen:
                continue

            normalized.append(token)
            seen.add(dedupe_key)

        return normalized


class JobUpdateRequest(BaseModel):
    title: str | None = Field(default=None, min_length=1)
    company: str | None = Field(default=None, min_length=1)
    location: str | None = None
    description: str | None = None
    requirements: str | None = None
    employment_type: str | None = Field(default=None, alias="employmentType")
    seniority_hint: str | None = Field(default=None, alias="seniorityHint")
    remote_policy: str | None = Field(default=None, alias="remotePolicy")
    tech_stack: list[str] | None = Field(default=None, alias="techStack")
    source: str | None = Field(default=None, min_length=1)
    source_url: str | None = Field(default=None, alias="sourceUrl")
    external_job_id: str | None = Field(default=None, alias="externalJobId")
    status: str | None = Field(default=None, min_length=1)

    model_config = {"populate_by_name": True}

    @field_validator(
        "title",
        "company",
        "location",
        "description",
        "requirements",
        "employment_type",
        "seniority_hint",
        "remote_policy",
        "source_url",
        "external_job_id",
    )
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None

        normalized = value.strip()

        return normalized or None

    @field_validator("source")
    @classmethod
    def normalize_optional_source(cls, value: str | None) -> str | None:
        if value is None:
            return None

        return value.strip().lower()

    @field_validator("status")
    @classmethod
    def normalize_optional_status(cls, value: str | None) -> str | None:
        if value is None:
            return None

        return value.strip().upper()

    @field_validator("tech_stack", mode="before")
    @classmethod
    def normalize_optional_tech_stack(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None

        if not isinstance(value, list):
            return []

        normalized: list[str] = []
        seen: set[str] = set()
        for item in value:
            token = str(item).strip()
            if not token:
                continue

            dedupe_key = token.lower()
            if dedupe_key in seen:
                continue

            normalized.append(token)
            seen.add(dedupe_key)

        return normalized


class JobData(TypedDict):
    jobId: int
    userId: int
    title: str
    company: str
    location: str | None
    description: str | None
    requirements: str | None
    employmentType: str | None
    seniorityHint: str | None
    remotePolicy: str | None
    techStack: list[str]
    ingestionRelevanceScore: float | None
    ingestionRelevanceReason: str | None
    ingestionExplorationKept: bool
    source: str
    sourceUrl: str | None
    externalJobId: str | None
    status: str
    createdAt: str
    updatedAt: str


def job_from_row(row: asyncpg.Record) -> JobData:
    raw_tech_stack = row["tech_stack"]
    if isinstance(raw_tech_stack, str):
        try:
            raw_tech_stack = json.loads(raw_tech_stack)
        except Exception:
            raw_tech_stack = []

    tech_stack = (
        [str(item).strip() for item in raw_tech_stack if str(item).strip()]
        if isinstance(raw_tech_stack, list)
        else []
    )

    ingestion_relevance_score = row["ingestion_relevance_score"]

    return {
        "jobId": row["id"],
        "userId": row["user_id"],
        "title": row["title"],
        "company": row["company"],
        "location": row["location"],
        "description": row["description"],
        "requirements": row["requirements"],
        "employmentType": row["employment_type"],
        "seniorityHint": row["seniority_hint"],
        "remotePolicy": row["remote_policy"],
        "techStack": tech_stack,
        "ingestionRelevanceScore": (
            float(ingestion_relevance_score)
            if ingestion_relevance_score is not None
            else None
        ),
        "ingestionRelevanceReason": row["ingestion_relevance_reason"],
        "ingestionExplorationKept": bool(row["ingestion_exploration_kept"]),
        "source": row["source"],
        "sourceUrl": row["source_url"],
        "externalJobId": row["external_job_id"],
        "status": row["status"],
        "createdAt": str(row["created_at"]),
        "updatedAt": str(row["updated_at"]),
    }
