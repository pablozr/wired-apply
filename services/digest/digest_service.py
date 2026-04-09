async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "Digest module base ready",
        "data": {"module": "digest"},
    }
