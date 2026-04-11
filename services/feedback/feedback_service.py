import asyncpg
from asyncpg.exceptions import UniqueViolationError

from core.logger.logger import logger
from schemas.feedback import FeedbackCreateRequest, FeedbackUpdateRequest, feedback_from_row
from services.rules import adaptive_weights, feedback_policy


def _weights_from_row(row: asyncpg.Record) -> dict[str, float]:
    return {
        "role_weight": float(row["role_weight"]),
        "salary_weight": float(row["salary_weight"]),
        "location_weight": float(row["location_weight"]),
        "seniority_weight": float(row["seniority_weight"]),
    }


def _weights_to_response(weights: dict[str, float]) -> dict[str, float]:
    return {
        "roleWeight": round(weights["role_weight"], 4),
        "salaryWeight": round(weights["salary_weight"], 4),
        "locationWeight": round(weights["location_weight"], 4),
        "seniorityWeight": round(weights["seniority_weight"], 4),
    }


async def _adjust_score_weights_from_feedback(
    conn: asyncpg.Connection,
    user_id: int,
    rating: int,
) -> dict[str, float]:
    await conn.execute(
        """
        INSERT INTO score_weights (user_id)
        VALUES ($1)
        ON CONFLICT (user_id) DO NOTHING
        """,
        user_id,
    )

    row = await conn.fetchrow(
        """
        SELECT
            role_weight,
            salary_weight,
            location_weight,
            seniority_weight
        FROM score_weights
        WHERE user_id = $1
        FOR UPDATE
        """,
        user_id,
    )

    if not row:
        return _weights_to_response(
            {
                "role_weight": 0.35,
                "salary_weight": 0.25,
                "location_weight": 0.2,
                "seniority_weight": 0.2,
            }
        )

    current_weights = _weights_from_row(row)
    impact = feedback_policy.feedback_impact_from_rating(rating)
    step = feedback_policy.delta_step_from_rating(rating)

    delta = adaptive_weights.build_delta_from_impact(
        current_weights,
        impact,
        step,
    )
    adjusted_weights = adaptive_weights.apply_delta_with_guardrails(
        current_weights,
        delta,
    )

    await conn.execute(
        """
        UPDATE score_weights
        SET
            role_weight = $1,
            salary_weight = $2,
            location_weight = $3,
            seniority_weight = $4,
            updated_at = NOW()
        WHERE user_id = $5
        """,
        adjusted_weights["role_weight"],
        adjusted_weights["salary_weight"],
        adjusted_weights["location_weight"],
        adjusted_weights["seniority_weight"],
        user_id,
    )

    return _weights_to_response(adjusted_weights)


async def create_feedback(
    conn: asyncpg.Connection,
    user_id: int,
    data: FeedbackCreateRequest,
) -> dict:
    if not feedback_policy.is_valid_feedback_rating(data.rating):
        return {"status": False, "message": "Invalid rating", "data": {}}

    try:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO user_feedback (
                    user_id,
                    job_id,
                    rating,
                    notes
                )
                SELECT
                    $1,
                    j.id,
                    $3,
                    $4
                FROM jobs j
                WHERE j.id = $2 AND j.user_id = $1
                RETURNING
                    id,
                    user_id,
                    job_id,
                    rating,
                    notes,
                    created_at,
                    updated_at
                """,
                user_id,
                data.job_id,
                data.rating,
                data.notes,
            )

            if not row:
                return {
                    "status": False,
                    "message": "Job not found for this user",
                    "data": {},
                }

            score_weights = await _adjust_score_weights_from_feedback(
                conn,
                user_id,
                data.rating,
            )

        return {
            "status": True,
            "message": "Feedback created successfully",
            "data": {
                "feedback": feedback_from_row(row),
                "scoreWeights": score_weights,
            },
        }
    except UniqueViolationError:
        return {
            "status": False,
            "message": "Feedback already exists for this job",
            "data": {},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def list_feedback(
    conn: asyncpg.Connection,
    user_id: int,
    limit: int = 20,
    offset: int = 0,
    rating: int | None = None,
) -> dict:
    safe_limit = max(1, min(limit, 100))
    safe_offset = max(0, offset)

    if rating is not None and not feedback_policy.is_valid_feedback_rating(rating):
        return {"status": False, "message": "Invalid rating filter", "data": {}}

    try:
        if rating is None:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    user_id,
                    job_id,
                    rating,
                    notes,
                    created_at,
                    updated_at
                FROM user_feedback
                WHERE user_id = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                """,
                user_id,
                safe_limit,
                safe_offset,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    user_id,
                    job_id,
                    rating,
                    notes,
                    created_at,
                    updated_at
                FROM user_feedback
                WHERE user_id = $1 AND rating = $2
                ORDER BY created_at DESC
                LIMIT $3 OFFSET $4
                """,
                user_id,
                rating,
                safe_limit,
                safe_offset,
            )

        feedbacks = [feedback_from_row(row) for row in rows]

        return {
            "status": True,
            "message": "Feedback list retrieved successfully",
            "data": {
                "feedback": feedbacks,
                "pagination": {
                    "limit": safe_limit,
                    "offset": safe_offset,
                    "count": len(feedbacks),
                },
            },
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_one_feedback(
    conn: asyncpg.Connection,
    user_id: int,
    feedback_id: int,
) -> dict:
    try:
        row = await conn.fetchrow(
            """
            SELECT
                id,
                user_id,
                job_id,
                rating,
                notes,
                created_at,
                updated_at
            FROM user_feedback
            WHERE id = $1 AND user_id = $2
            """,
            feedback_id,
            user_id,
        )

        if not row:
            return {"status": False, "message": "Feedback not found", "data": {}}

        return {
            "status": True,
            "message": "Feedback retrieved successfully",
            "data": {"feedback": feedback_from_row(row)},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def update_feedback(
    conn: asyncpg.Connection,
    user_id: int,
    feedback_id: int,
    data: FeedbackUpdateRequest,
) -> dict:
    allowed_columns = {"rating", "notes"}
    filtered = {
        key: value
        for key, value in data.model_dump(exclude_none=True).items()
        if key in allowed_columns
    }

    if not filtered:
        return {"status": False, "message": "No fields to update", "data": {}}

    if "rating" in filtered and not feedback_policy.is_valid_feedback_rating(filtered["rating"]):
        return {"status": False, "message": "Invalid rating", "data": {}}

    try:
        async with conn.transaction():
            columns = list(filtered.keys())
            values = list(filtered.values())
            set_clause = ", ".join(
                f"{column} = ${index}" for index, column in enumerate(columns, 1)
            )

            values.extend([feedback_id, user_id])

            query = f"""
                UPDATE user_feedback SET {set_clause}, updated_at = NOW()
                WHERE id = ${len(values) - 1} AND user_id = ${len(values)}
                RETURNING
                    id,
                    user_id,
                    job_id,
                    rating,
                    notes,
                    created_at,
                    updated_at
            """

            row = await conn.fetchrow(query, *values)

            if not row:
                return {
                    "status": False,
                    "message": "Feedback not found",
                    "data": {},
                }

            score_weights = await _adjust_score_weights_from_feedback(
                conn,
                user_id,
                row["rating"],
            )
            response_data = {
                "feedback": feedback_from_row(row),
                "scoreWeights": score_weights,
            }

        return {
            "status": True,
            "message": "Feedback updated successfully",
            "data": response_data,
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def delete_feedback(
    conn: asyncpg.Connection,
    user_id: int,
    feedback_id: int,
) -> dict:
    try:
        result = await conn.execute(
            "DELETE FROM user_feedback WHERE id = $1 AND user_id = $2",
            feedback_id,
            user_id,
        )

        if result == "DELETE 0":
            return {"status": False, "message": "Feedback not found", "data": {}}

        return {
            "status": True,
            "message": "Feedback deleted successfully",
            "data": {"feedbackId": feedback_id},
        }
    except Exception as e:
        logger.exception(e)
        return {"status": False, "message": "Internal server error", "data": {}}


async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "Feedback module base ready",
        "data": {"module": "feedback"},
    }
