from datetime import date
from typing import TypedDict

import asyncpg
from pydantic import BaseModel, Field


class DigestGenerateRequest(BaseModel):
    digest_date: date | None = Field(default=None, alias="digestDate")

    model_config = {"populate_by_name": True}


class DigestData(TypedDict):
    digestId: int
    userId: int
    digestDate: str
    totalJobs: int
    totalApplications: int
    totalInterviews: int
    payload: dict
    createdAt: str
    updatedAt: str


def digest_from_row(row: asyncpg.Record) -> DigestData:
    return {
        "digestId": row["id"],
        "userId": row["user_id"],
        "digestDate": str(row["digest_date"]),
        "totalJobs": row["total_jobs"],
        "totalApplications": row["total_applications"],
        "totalInterviews": row["total_interviews"],
        "payload": row["payload"],
        "createdAt": str(row["created_at"]),
        "updatedAt": str(row["updated_at"]),
    }
