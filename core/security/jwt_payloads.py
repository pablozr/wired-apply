import asyncpg


def auth_jwt_payload_from_row(row: asyncpg.Record) -> dict:
    return {
        "userId": row["id"],
        "email": row["email"],
        "fullname": row["fullname"],
        "role": row["role"],
        "type": "auth",
    }


def reset_jwt_payload(
    user_id: int,
    email: str,
    fullname: str,
    role: str,
    *,
    can_update: bool,
) -> dict:
    return {
        "userId": user_id,
        "email": email,
        "fullname": fullname,
        "role": role,
        "type": "reset",
        "canUpdate": can_update,
    }
