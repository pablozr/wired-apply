async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "ATS integration base ready",
        "data": {"module": "ats"},
    }
