async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "Ranking module base ready",
        "data": {"module": "ranking"},
    }
