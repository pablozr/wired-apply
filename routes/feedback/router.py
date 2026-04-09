from fastapi import APIRouter

from functions.utils.utils import default_response
from services.feedback import feedback_service

router = APIRouter()


@router.get("/health")
async def feedback_health():
    return await default_response(feedback_service.get_module_status)
