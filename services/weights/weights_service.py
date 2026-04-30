import asyncpg

DEFAULT_SCORE_WEIGHTS = {
    "role_weight": 0.50,
    "salary_weight": 0.10,
    "location_weight": 0.25,
    "seniority_weight": 0.15,
}


def weights_from_row(row: asyncpg.Record) -> dict[str, float]:
    return {
        "role_weight": float(row["role_weight"]),
        "salary_weight": float(row["salary_weight"]),
        "location_weight": float(row["location_weight"]),
        "seniority_weight": float(row["seniority_weight"]),
    }


def weights_to_response(weights: dict[str, float]) -> dict[str, float]:
    return {
        "roleWeight": round(weights["role_weight"], 4),
        "salaryWeight": round(weights["salary_weight"], 4),
        "locationWeight": round(weights["location_weight"], 4),
        "seniorityWeight": round(weights["seniority_weight"], 4),
    }


async def get_score_weights(conn: asyncpg.Connection, user_id: int) -> dict[str, float]:
    row = await conn.fetchrow(
        """
        SELECT
            role_weight,
            salary_weight,
            location_weight,
            seniority_weight
        FROM score_weights
        WHERE user_id = $1
        """,
        user_id,
    )

    if not row:
        return DEFAULT_SCORE_WEIGHTS.copy()

    return weights_from_row(row)
