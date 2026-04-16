import json
from typing import Any


def ensure_list(value: Any) -> list:
    if isinstance(value, list):
        return value

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []

        return parsed if isinstance(parsed, list) else []

    return []


def ensure_str_list(value: Any) -> list[str]:
    return [str(item).strip() for item in ensure_list(value) if str(item).strip()]


def ensure_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}

        return parsed if isinstance(parsed, dict) else {}

    return {}
