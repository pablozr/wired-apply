from typing import TypedDict

import asyncpg
from pydantic import BaseModel, Field, field_validator


class JobCreateRequest(BaseModel):
    title: str = Field(min_length=1)
    company: str = Field(min_length=1)
    location: str | None = None
    source: str = Field(default="manual", min_length=1)
    source_url: str | None = Field(default=None, alias="sourceUrl")
    external_job_id: str | None = Field(default=None, alias="externalJobId")
    status: str = Field(default="NEW", min_length=1)

    model_config = {"populate_by_name": True}

    @field_validator("title", "company", "location", "source_url", "external_job_id")
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


class JobUpdateRequest(BaseModel):
    title: str | None = Field(default=None, min_length=1)
    company: str | None = Field(default=None, min_length=1)
    location: str | None = None
    source: str | None = Field(default=None, min_length=1)
    source_url: str | None = Field(default=None, alias="sourceUrl")
    external_job_id: str | None = Field(default=None, alias="externalJobId")
    status: str | None = Field(default=None, min_length=1)

    model_config = {"populate_by_name": True}

    @field_validator("title", "company", "location", "source_url", "external_job_id")
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


class JobData(TypedDict):
    jobId: int
    userId: int
    title: str
    company: str
    location: str | None
    source: str
    sourceUrl: str | None
    externalJobId: str | None
    status: str
    createdAt: str
    updatedAt: str


def job_from_row(row: asyncpg.Record) -> JobData:
    return {
        "jobId": row["id"],
        "userId": row["user_id"],
        "title": row["title"],
        "company": row["company"],
        "location": row["location"],
        "source": row["source"],
        "sourceUrl": row["source_url"],
        "externalJobId": row["external_job_id"],
        "status": row["status"],
        "createdAt": str(row["created_at"]),
        "updatedAt": str(row["updated_at"]),
    }
