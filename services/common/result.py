from typing import Any

from core.logger.logger import logger


ServiceResult = dict[str, Any]


def service_success(message: str, data: dict | None = None) -> ServiceResult:
    return {
        "status": True,
        "message": message,
        "data": data or {},
    }


def service_error(message: str, data: dict | None = None) -> ServiceResult:
    return {
        "status": False,
        "message": message,
        "data": data or {},
    }


def internal_error(error: Exception, message: str = "Internal server error") -> ServiceResult:
    logger.exception(error)
    return service_error(message)
