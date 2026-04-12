import json
from datetime import date
from typing import Any
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
    payload: dict[str, Any]
    createdAt: str
    updatedAt: str


def digest_from_row(row: asyncpg.Record) -> DigestData:
    raw_payload = row["payload"]

    if isinstance(raw_payload, str):
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            payload = {}
    elif isinstance(raw_payload, dict):
        payload = raw_payload
    else:
        payload = {}

    return {
        "digestId": row["id"],
        "userId": row["user_id"],
        "digestDate": str(row["digest_date"]),
        "totalJobs": row["total_jobs"],
        "totalApplications": row["total_applications"],
        "totalInterviews": row["total_interviews"],
        "payload": payload,
        "createdAt": str(row["created_at"]),
        "updatedAt": str(row["updated_at"]),
    }
