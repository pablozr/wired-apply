AUTO_APPLY_ALLOWED_STATUSES = {"APPLY_READY", "APPROVED"}


def can_auto_apply(status: str) -> bool:
    return status.strip().upper() in AUTO_APPLY_ALLOWED_STATUSES
