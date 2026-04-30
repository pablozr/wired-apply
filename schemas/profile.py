import json
from typing import TypedDict

import asyncpg
from pydantic import BaseModel, Field, field_validator


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip()
    return normalized or None


def _normalize_list(values: list[str] | None) -> list[str]:
    if not values:
        return []

    normalized: list[str] = []
    seen: set[str] = set()

    for value in values:
        item = str(value or "").strip()
        if not item:
            continue

        dedupe_key = item.lower()
        if dedupe_key in seen:
            continue

        normalized.append(item)
        seen.add(dedupe_key)

    return normalized


def _coerce_to_list(value) -> list[str]:
    if value is None:
        return []

    if isinstance(value, list):
        return [str(item) for item in value]

    if isinstance(value, str):
        normalized = value.replace("\r", "\n").replace(";", ",")
        if not normalized.strip():
            return []

        parts = []
        for line in normalized.split("\n"):
            for token in line.split(","):
                item = token.strip()
                if item:
                    parts.append(item)

        return parts

    return []


def _ensure_list(value) -> list[str]:
    if value is None:
        return []

    parsed = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            parsed = []

    if not isinstance(parsed, list):
        return []

    return _normalize_list([str(item) for item in parsed])


class UserProfileUpsertRequest(BaseModel):
    objective: str | None = None
    seniority: str | None = None
    target_roles: list[str] = Field(default_factory=list, alias="targetRoles")
    preferred_locations: list[str] = Field(
        default_factory=list,
        alias="preferredLocations",
    )
    preferred_work_model: str | None = Field(default=None, alias="preferredWorkModel")
    salary_expectation: str | None = Field(default=None, alias="salaryExpectation")
    must_have_skills: list[str] = Field(default_factory=list, alias="mustHaveSkills")
    nice_to_have_skills: list[str] = Field(
        default_factory=list,
        alias="niceToHaveSkills",
    )

    model_config = {"populate_by_name": True}

    @field_validator(
        "objective",
        "salary_expectation",
        mode="before",
    )
    @classmethod
    def normalize_optional_text_fields(cls, value: str | None) -> str | None:
        return _normalize_optional_text(value)

    @field_validator("seniority", "preferred_work_model", mode="before")
    @classmethod
    def normalize_optional_uppercase_fields(cls, value: str | None) -> str | None:
        normalized = _normalize_optional_text(value)
        if normalized is None:
            return None

        return normalized.upper()

    @field_validator(
        "target_roles",
        "preferred_locations",
        "must_have_skills",
        "nice_to_have_skills",
        mode="before",
    )
    @classmethod
    def normalize_list_fields(cls, value: list[str] | None) -> list[str]:
        return _normalize_list(_coerce_to_list(value))


class UserProfileData(TypedDict):
    userId: int
    objective: str | None
    seniority: str | None
    targetRoles: list[str]
    preferredLocations: list[str]
    preferredWorkModel: str | None
    salaryExpectation: str | None
    mustHaveSkills: list[str]
    niceToHaveSkills: list[str]
    createdAt: str | None
    updatedAt: str | None


def profile_from_row(row: asyncpg.Record) -> UserProfileData:
    return {
        "userId": row["user_id"],
        "objective": row["objective"],
        "seniority": row["seniority"],
        "targetRoles": _ensure_list(row["target_roles"]),
        "preferredLocations": _ensure_list(row["preferred_locations"]),
        "preferredWorkModel": row["preferred_work_model"],
        "salaryExpectation": row["salary_expectation"],
        "mustHaveSkills": _ensure_list(row["must_have_skills"]),
        "niceToHaveSkills": _ensure_list(row["nice_to_have_skills"]),
        "createdAt": str(row["created_at"]) if row["created_at"] else None,
        "updatedAt": str(row["updated_at"]) if row["updated_at"] else None,
    }


def empty_profile(user_id: int) -> UserProfileData:
    return {
        "userId": user_id,
        "objective": None,
        "seniority": None,
        "targetRoles": [],
        "preferredLocations": [],
        "preferredWorkModel": None,
        "salaryExpectation": None,
        "mustHaveSkills": [],
        "niceToHaveSkills": [],
        "createdAt": None,
        "updatedAt": None,
    }
