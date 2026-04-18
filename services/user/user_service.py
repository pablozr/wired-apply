import hashlib
import json

import asyncpg
from asyncpg.exceptions import UniqueViolationError
from fastapi import UploadFile

from core.config.config import RESUME_ALLOWED_MIME_TYPES, RESUME_MAX_FILE_SIZE_BYTES
from core.logger.logger import logger
from core.security.hashing import hash_password
from schemas.profile import UserProfileUpsertRequest, empty_profile, profile_from_row
from schemas.resume import resume_from_row
from schemas.user import UserCreateRequest, UserUpdateRequest, user_from_row
from services.ai import ai_service


async def get_one_user(conn: asyncpg.Connection, user_id: int) -> dict:
    try:
        row = await conn.fetchrow(
            "SELECT id, fullname, email, role, created_at FROM users WHERE id = $1",
            user_id,
        )

        if not row:
            return {"status": False, "message": "User not found", "data": {}}

        return {
            "status": True,
            "message": "User retrieved successfully",
            "data": {"user": user_from_row(row)},
        }
    except Exception as error:
        logger.exception(error)
        return {"status": False, "message": "Internal server error", "data": {}}


async def create(conn: asyncpg.Connection, data: UserCreateRequest) -> dict:
    try:
        already_exists = await conn.fetchval(
            "SELECT 1 FROM users WHERE LOWER(email) = LOWER($1) LIMIT 1",
            data.email,
        )
        if already_exists:
            return {"status": False, "message": "Email already registered", "data": {}}

        hashed = hash_password(data.password)

        row = await conn.fetchrow(
            """
            INSERT INTO users (fullname, email, password, role)
            VALUES ($1, $2, $3, 'BASIC')
            RETURNING id, fullname, email, role, created_at
            """,
            data.fullname,
            data.email,
            hashed,
        )

        return {
            "status": True,
            "message": "User created successfully",
            "data": {"user": user_from_row(row)},
        }
    except UniqueViolationError:
        return {"status": False, "message": "Email already registered", "data": {}}
    except Exception as error:
        logger.exception(error)
        return {"status": False, "message": "Internal server error", "data": {}}


async def update_me(
    conn: asyncpg.Connection,
    user_id: int,
    data: UserUpdateRequest,
) -> dict:
    allowed_columns = {"fullname", "email"}
    filtered = {
        key: value
        for key, value in data.model_dump(exclude_none=True).items()
        if key in allowed_columns
    }

    if not filtered:
        return {"status": False, "message": "No fields to update", "data": {}}

    try:
        columns = list(filtered.keys())
        values = list(filtered.values())
        set_clause = ", ".join(f"{column} = ${index}" for index, column in enumerate(columns, 1))
        values.append(user_id)

        query = f"""
            UPDATE users SET {set_clause}, updated_at = NOW()
            WHERE id = ${len(values)}
            RETURNING id, fullname, email, role, created_at
        """
        row = await conn.fetchrow(query, *values)

        if not row:
            return {"status": False, "message": "User not found", "data": {}}

        return {
            "status": True,
            "message": "User updated successfully",
            "data": {"user": user_from_row(row)},
        }
    except UniqueViolationError:
        return {"status": False, "message": "Email already in use", "data": {}}
    except Exception as error:
        logger.exception(error)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_my_profile(conn: asyncpg.Connection, user_id: int) -> dict:
    try:
        row = await conn.fetchrow(
            """
            SELECT
                user_id,
                objective,
                seniority,
                target_roles,
                preferred_locations,
                preferred_work_model,
                salary_expectation,
                must_have_skills,
                nice_to_have_skills,
                created_at,
                updated_at
            FROM user_profiles
            WHERE user_id = $1
            """,
            user_id,
        )

        profile = profile_from_row(row) if row else empty_profile(user_id)

        return {
            "status": True,
            "message": "User profile retrieved successfully",
            "data": {"profile": profile},
        }
    except Exception as error:
        logger.exception(error)
        return {"status": False, "message": "Internal server error", "data": {}}


async def upsert_my_profile(
    conn: asyncpg.Connection,
    user_id: int,
    data: UserProfileUpsertRequest,
) -> dict:
    payload = data.model_dump(by_alias=False)

    try:
        row = await conn.fetchrow(
            """
            INSERT INTO user_profiles (
                user_id,
                objective,
                seniority,
                target_roles,
                preferred_locations,
                preferred_work_model,
                salary_expectation,
                must_have_skills,
                nice_to_have_skills
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (user_id)
            DO UPDATE SET
                objective = EXCLUDED.objective,
                seniority = EXCLUDED.seniority,
                target_roles = EXCLUDED.target_roles,
                preferred_locations = EXCLUDED.preferred_locations,
                preferred_work_model = EXCLUDED.preferred_work_model,
                salary_expectation = EXCLUDED.salary_expectation,
                must_have_skills = EXCLUDED.must_have_skills,
                nice_to_have_skills = EXCLUDED.nice_to_have_skills,
                updated_at = NOW()
            RETURNING
                user_id,
                objective,
                seniority,
                target_roles,
                preferred_locations,
                preferred_work_model,
                salary_expectation,
                must_have_skills,
                nice_to_have_skills,
                created_at,
                updated_at
            """,
            user_id,
            payload.get("objective"),
            payload.get("seniority"),
            json.dumps(payload.get("target_roles") or []),
            json.dumps(payload.get("preferred_locations") or []),
            payload.get("preferred_work_model"),
            payload.get("salary_expectation"),
            json.dumps(payload.get("must_have_skills") or []),
            json.dumps(payload.get("nice_to_have_skills") or []),
        )

        return {
            "status": True,
            "message": "User profile saved successfully",
            "data": {"profile": profile_from_row(row)},
        }
    except Exception as error:
        logger.exception(error)
        return {"status": False, "message": "Internal server error", "data": {}}


def _validate_resume_file(file_name: str, mime_type: str, file_size: int) -> str | None:
    if not file_name.lower().endswith(".pdf"):
        return "Only PDF files are supported"

    normalized_mime = mime_type.lower()
    if (
        RESUME_ALLOWED_MIME_TYPES
        and normalized_mime not in RESUME_ALLOWED_MIME_TYPES
        and normalized_mime != "application/octet-stream"
    ):
        return "Invalid resume file type"

    if file_size <= 0:
        return "Resume file is empty"

    if file_size > RESUME_MAX_FILE_SIZE_BYTES:
        return f"Resume file exceeds limit of {RESUME_MAX_FILE_SIZE_BYTES} bytes"

    return None


async def upload_my_resume(
    conn: asyncpg.Connection,
    user_id: int,
    file: UploadFile,
) -> dict:
    file_name = (file.filename or "resume.pdf").strip() or "resume.pdf"
    mime_type = (file.content_type or "application/pdf").strip().lower()

    try:
        file_bytes = await file.read()
        file_size = len(file_bytes)

        validation_error = _validate_resume_file(file_name, mime_type, file_size)
        if validation_error:
            return {"status": False, "message": validation_error, "data": {}}

        file_hash = hashlib.sha256(file_bytes).hexdigest()

        parse_response = await ai_service.parse_resume_pdf(file_bytes, file_name)
        parse_data = parse_response.get("data", {})
        parse_status = str(parse_data.get("parseStatus") or "FAILED").strip().upper()
        parse_confidence = parse_data.get("parseConfidence")

        try:
            parse_confidence = (
                max(0.0, min(1.0, float(parse_confidence)))
                if parse_confidence is not None
                else None
            )
        except (TypeError, ValueError):
            parse_confidence = None

        extracted_text = parse_data.get("extractedText")
        extracted_json = parse_data.get("extractedJson")
        if not isinstance(extracted_json, dict):
            extracted_json = {}

        async with conn.transaction():
            existing_row = await conn.fetchrow(
                """
                SELECT
                    id,
                    user_id,
                    file_name,
                    mime_type,
                    file_size,
                    file_hash,
                    parse_status,
                    parse_confidence,
                    extracted_text,
                    extracted_json,
                    is_active,
                    created_at,
                    updated_at
                FROM user_resumes
                WHERE user_id = $1 AND is_active = TRUE AND file_hash = $2
                ORDER BY created_at DESC
                LIMIT 1
                """,
                user_id,
                file_hash,
            )

            if existing_row:
                return {
                    "status": True,
                    "message": "Resume already uploaded",
                    "data": {"resume": resume_from_row(existing_row)},
                }

            await conn.execute(
                """
                UPDATE user_resumes
                SET is_active = FALSE, updated_at = NOW()
                WHERE user_id = $1 AND is_active = TRUE
                """,
                user_id,
            )

            row = await conn.fetchrow(
                """
                INSERT INTO user_resumes (
                    user_id,
                    file_name,
                    mime_type,
                    file_size,
                    file_hash,
                    file_content,
                    extracted_text,
                    extracted_json,
                    parse_status,
                    parse_confidence,
                    is_active
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE)
                RETURNING
                    id,
                    user_id,
                    file_name,
                    mime_type,
                    file_size,
                    file_hash,
                    parse_status,
                    parse_confidence,
                    extracted_text,
                    extracted_json,
                    is_active,
                    created_at,
                    updated_at
                """,
                user_id,
                file_name,
                mime_type,
                file_size,
                file_hash,
                file_bytes,
                extracted_text,
                json.dumps(extracted_json),
                parse_status,
                parse_confidence,
            )

        if parse_status == "COMPLETED":
            message = "Resume uploaded and parsed successfully"
        elif parse_status == "FALLBACK":
            message = "Resume uploaded with fallback parse"
        else:
            message = "Resume uploaded but parse failed"

        return {
            "status": True,
            "message": message,
            "data": {"resume": resume_from_row(row)},
        }
    except Exception as error:
        logger.exception(error)
        return {"status": False, "message": "Internal server error", "data": {}}
    finally:
        await file.close()


async def get_my_resume(conn: asyncpg.Connection, user_id: int) -> dict:
    try:
        row = await conn.fetchrow(
            """
            SELECT
                id,
                user_id,
                file_name,
                mime_type,
                file_size,
                file_hash,
                parse_status,
                parse_confidence,
                extracted_text,
                extracted_json,
                is_active,
                created_at,
                updated_at
            FROM user_resumes
            WHERE user_id = $1 AND is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user_id,
        )

        if not row:
            return {"status": False, "message": "Resume not found", "data": {}}

        return {
            "status": True,
            "message": "Resume retrieved successfully",
            "data": {"resume": resume_from_row(row)},
        }
    except Exception as error:
        logger.exception(error)
        return {"status": False, "message": "Internal server error", "data": {}}


async def delete_my_resume(conn: asyncpg.Connection, user_id: int) -> dict:
    try:
        result = await conn.execute(
            "DELETE FROM user_resumes WHERE user_id = $1 AND is_active = TRUE",
            user_id,
        )

        if result == "DELETE 0":
            return {"status": False, "message": "Resume not found", "data": {}}

        return {
            "status": True,
            "message": "Resume deleted successfully",
            "data": {"deleted": True},
        }
    except Exception as error:
        logger.exception(error)
        return {"status": False, "message": "Internal server error", "data": {}}
