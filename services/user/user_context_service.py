import asyncpg

from core.utils.json_utils import ensure_dict, ensure_str_list


async def get_profile_context(conn: asyncpg.Connection, user_id: int) -> dict:
    row = await conn.fetchrow(
        """
        SELECT
            objective,
            seniority,
            target_roles,
            preferred_locations,
            preferred_work_model,
            salary_expectation,
            must_have_skills,
            nice_to_have_skills
        FROM user_profiles
        WHERE user_id = $1
        """,
        user_id,
    )

    if not row:
        return {}

    return {
        "objective": row["objective"],
        "seniority": row["seniority"],
        "targetRoles": ensure_str_list(row["target_roles"]),
        "preferredLocations": ensure_str_list(row["preferred_locations"]),
        "preferredWorkModel": row["preferred_work_model"],
        "salaryExpectation": row["salary_expectation"],
        "mustHaveSkills": ensure_str_list(row["must_have_skills"]),
        "niceToHaveSkills": ensure_str_list(row["nice_to_have_skills"]),
    }


async def get_resume_context(conn: asyncpg.Connection, user_id: int) -> dict:
    row = await conn.fetchrow(
        """
        SELECT
            extracted_json,
            parse_status,
            parse_confidence
        FROM user_resumes
        WHERE user_id = $1 AND is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
        """,
        user_id,
    )

    if not row:
        return {}

    context = ensure_dict(row["extracted_json"])
    context["parseStatus"] = row["parse_status"]
    parse_confidence = row["parse_confidence"]
    context["parseConfidence"] = (
        float(parse_confidence) if parse_confidence is not None else None
    )
    return context


async def get_ai_context(conn: asyncpg.Connection, user_id: int) -> tuple[dict, dict]:
    return (
        await get_profile_context(conn, user_id),
        await get_resume_context(conn, user_id),
    )
