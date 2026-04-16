from core.logger.logger import logger


async def prepare_assisted_apply(
    run_id: str,
    user_id: int,
    job_id: int,
    job_context: dict,
    auto_apply_payload: dict,
) -> dict:
    source_url = job_context.get("sourceUrl")
    answers = auto_apply_payload.get("answers")

    if not source_url:
        return {
            "status": False,
            "message": "Job source URL is missing for assisted apply",
            "data": {},
        }

    answers_count = len(answers) if isinstance(answers, dict) else 0

    logger.info(
        "playwright_assisted_plan_ready run_id=%s user_id=%s job_id=%s answers=%s",
        run_id,
        user_id,
        job_id,
        answers_count,
    )

    return {
        "status": True,
        "message": "Playwright assisted apply plan prepared",
        "data": {
            "mode": "ASSISTED",
            "sourceUrl": source_url,
            "answersCount": answers_count,
            "nextStep": "human_confirmation_required",
        },
    }


async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "Playwright integration ready in assisted mode",
        "data": {
            "module": "playwright",
            "mode": "ASSISTED",
            "requiresHumanConfirmation": True,
        },
    }
