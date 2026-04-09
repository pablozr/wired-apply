async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "Read model module base ready",
        "data": {"module": "read_model"},
    }
