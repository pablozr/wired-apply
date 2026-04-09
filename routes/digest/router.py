from fastapi import APIRouter

from functions.utils.utils import default_response
from services.digest import digest_service

router = APIRouter()


@router.get("/health")
async def digest_health():
    return await default_response(digest_service.get_module_status)
