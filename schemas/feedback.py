from typing import TypedDict

import asyncpg
from pydantic import BaseModel, Field, field_validator


class FeedbackCreateRequest(BaseModel):
    job_id: int = Field(alias="jobId")
    rating: int = Field(ge=1, le=5)
    notes: str | None = None

    model_config = {"populate_by_name": True}

    @field_validator("notes")
    @classmethod
    def normalize_optional_notes(cls, value: str | None) -> str | None:
        if value is None:
            return None

        normalized = value.strip()

        return normalized or None


class FeedbackUpdateRequest(BaseModel):
    rating: int | None = Field(default=None, ge=1, le=5)
    notes: str | None = None

    @field_validator("notes")
    @classmethod
    def normalize_optional_notes(cls, value: str | None) -> str | None:
        if value is None:
            return None

        normalized = value.strip()

        return normalized or None


class FeedbackData(TypedDict):
    feedbackId: int
    userId: int
    jobId: int
    rating: int
    notes: str | None
    createdAt: str
    updatedAt: str


def feedback_from_row(row: asyncpg.Record) -> FeedbackData:
    return {
        "feedbackId": row["id"],
        "userId": row["user_id"],
        "jobId": row["job_id"],
        "rating": row["rating"],
        "notes": row["notes"],
        "createdAt": str(row["created_at"]),
        "updatedAt": str(row["updated_at"]),
    }
