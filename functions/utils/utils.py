import secrets
import string

from fastapi.responses import JSONResponse


async def default_response(callable_function, params=None, is_creation=False):
    if params is None:
        params = []

    result = await callable_function(*params)

    if not result["status"]:
        return JSONResponse(status_code=400, content={"detail": result["message"]})

    status_code = 201 if is_creation else 200

    return JSONResponse(
        status_code=status_code,
        content={"message": result["message"], "data": result["data"]},
    )


def serialize_row(
    row: dict,
    date_fields: list[str] | None = None,
    decimal_fields: list[str] | None = None,
) -> dict:
    date_fields = date_fields or []
    decimal_fields = decimal_fields or []

    result = dict(row)

    for f in date_fields:
        if result.get(f) is not None:
            result[f] = str(result[f])

    for f in decimal_fields:
        if result.get(f) is not None:
            result[f] = float(result[f])

    return result


def generate_temp_code() -> str:
    return "".join(secrets.choice(string.digits) for _ in range(6))
