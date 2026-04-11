from typing import TypedDict, Optional

import asyncpg
from pydantic import BaseModel, Field, field_validator


class ApplicationCreateRequest(BaseModel):
    job_id: int = Field(alias="jobId")
    status: str = Field(default="PENDING", min_length=1)
    channel: str = Field(default="MANUAL", min_length=1)
    notes: Optional[str] = None

    model_config = {"populate_by_name": True}

    @field_validator("status", "channel")
    @classmethod
    def normalize_uppercase(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("notes")
    @classmethod
    def normalize_optional_notes(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None

        normalized = value.strip()

        return normalized or None


class ApplicationUpdateRequest(BaseModel):
    status: Optional[str] = Field(default=None, min_length=1)
    notes: Optional[str] = None

    @field_validator("status")
    @classmethod
    def normalize_optional_uppercase(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None

        return value.strip().upper()

    @field_validator("notes")
    @classmethod
    def normalize_optional_notes(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None

        normalized = value.strip()

        return normalized or None


class ApplicationData(TypedDict):
    applicationId: int
    userId: int
    jobId: int
    status: str
    channel: str
    notes: Optional[str]
    appliedAt: Optional[str]
    createdAt: str
    updatedAt: str


def application_from_row(row: asyncpg.Record) -> ApplicationData:
    return {
        "applicationId": row["id"],
        "userId": row["user_id"],
        "jobId": row["job_id"],
        "status": row["status"],
        "channel": row["channel"],
        "notes": row["notes"],
        "appliedAt": str(row["applied_at"]) if row["applied_at"] else None,
        "createdAt": str(row["created_at"]),
        "updatedAt": str(row["updated_at"]),
    }
