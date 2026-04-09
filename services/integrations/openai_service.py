async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "OpenAI integration base ready",
        "data": {"module": "openai"},
    }
