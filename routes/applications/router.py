from fastapi import APIRouter

from functions.utils.utils import default_response
from services.applications import application_service

router = APIRouter()


@router.get("/health")
async def applications_health():
    return await default_response(application_service.get_module_status)
