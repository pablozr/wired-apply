from typing import TypedDict

import asyncpg
from pydantic import BaseModel, Field, field_validator


class UserCreateRequest(BaseModel):
    fullname: str = Field(min_length=1, alias="fullName")
    email: str = Field(min_length=5)
    password: str = Field(min_length=6)

    model_config = {"populate_by_name": True}

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        return v.strip().lower()


class UserUpdateRequest(BaseModel):
    fullname: str | None = Field(default=None, min_length=1, alias="fullName")
    email: str | None = Field(default=None, min_length=5)

    model_config = {"populate_by_name": True}

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str | None) -> str | None:
        return v.strip().lower() if v else v


class UserData(TypedDict):
    userId: int
    fullname: str
    email: str
    role: str
    createdAt: str


class UserGetResponse(TypedDict):
    status: bool
    message: str
    data: UserData


def user_from_row(row: asyncpg.Record) -> UserData:
    return {
        "userId": row["id"],
        "fullname": row["fullname"],
        "email": row["email"],
        "role": row["role"],
        "createdAt": str(row["created_at"]),
    }
