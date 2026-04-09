async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "SMTP integration base ready",
        "data": {"module": "smtp"},
    }
