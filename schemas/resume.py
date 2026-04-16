import json
from typing import TypedDict

import asyncpg


def _ensure_dict(value) -> dict:
    if value is None:
        return {}

    if isinstance(value, dict):
        return value

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    return {}


class ResumeData(TypedDict):
    resumeId: int
    userId: int
    fileName: str
    mimeType: str
    fileSize: int
    fileHash: str
    parseStatus: str
    parseConfidence: float | None
    extractedText: str | None
    extractedJson: dict
    isActive: bool
    createdAt: str
    updatedAt: str


def resume_from_row(row: asyncpg.Record) -> ResumeData:
    parse_confidence = row["parse_confidence"]

    return {
        "resumeId": row["id"],
        "userId": row["user_id"],
        "fileName": row["file_name"],
        "mimeType": row["mime_type"],
        "fileSize": int(row["file_size"]),
        "fileHash": row["file_hash"],
        "parseStatus": row["parse_status"],
        "parseConfidence": float(parse_confidence) if parse_confidence is not None else None,
        "extractedText": row["extracted_text"],
        "extractedJson": _ensure_dict(row["extracted_json"]),
        "isActive": bool(row["is_active"]),
        "createdAt": str(row["created_at"]),
        "updatedAt": str(row["updated_at"]),
    }
