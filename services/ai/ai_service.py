import asyncio
import json
import re

from core.config.config import (
    AI_REQUEST_TIMEOUT_SECONDS,
    AI_SCORE_DESCRIPTION_MAX_CHARS,
    AI_SCORE_REQUIREMENTS_MAX_CHARS,
    AI_SCORE_SUMMARY_MAX_CHARS,
    RESUME_AI_MAX_INPUT_CHARS,
    RESUME_MIN_EXTRACTED_TEXT_CHARS,
)
from core.logger.logger import logger
from functions.pypdf.pdf_utils import extract_text_from_pdf_bytes
from prompts.ai_prompts import AUTO_APPLY_PROMPT, RESUME_PARSE_PROMPT, SCORING_PROMPT
from services.integrations.tyr_agent_client import get_tyr_agent


def _parse_agent_json(raw_text: str | None) -> dict:
    if not raw_text:
        return {}

    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(cleaned)
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\}", cleaned)
    if not match:
        return {}

    try:
        return json.loads(match.group(0))
    except Exception:
        return {}


def _normalize_confidence(value):
    try:
        confidence = float(value)
        if confidence > 1.0 and confidence <= 100.0:
            confidence = confidence / 100.0
        return max(0.0, min(1.0, confidence))
    except Exception:
        return None


async def parse_resume_pdf(file_bytes: bytes, file_name: str) -> dict:
    extracted_text = extract_text_from_pdf_bytes(file_bytes)
    if len(extracted_text) < RESUME_MIN_EXTRACTED_TEXT_CHARS:
        return {
            "status": False,
            "message": "Failed to extract text from PDF",
            "data": {
                "extractedText": extracted_text,
                "extractedJson": {},
                "parseStatus": "FAILED",
                "parseConfidence": None,
            },
        }

    text_lower = extracted_text.lower()
    if "staff" in text_lower:
        fallback_seniority = "STAFF"
    elif "lead" in text_lower or "lider" in text_lower:
        fallback_seniority = "LEAD"
    elif "senior" in text_lower:
        fallback_seniority = "SENIOR"
    elif "junior" in text_lower:
        fallback_seniority = "JUNIOR"
    elif "pleno" in text_lower or "mid" in text_lower or "middle" in text_lower:
        fallback_seniority = "MID"
    else:
        fallback_seniority = None

    skills_keywords = [
        "python",
        "django",
        "fastapi",
        "flask",
        "postgresql",
        "mysql",
        "redis",
        "rabbitmq",
        "docker",
        "kubernetes",
        "aws",
        "gcp",
        "azure",
        "javascript",
        "typescript",
        "react",
        "node",
        "sql",
        "git",
        "playwright",
    ]

    fallback_languages: list[str] = []
    if "english" in text_lower or "ingles" in text_lower:
        fallback_languages.append("English")
    if "portuguese" in text_lower or "portugues" in text_lower:
        fallback_languages.append("Portuguese")
    if "spanish" in text_lower or "espanhol" in text_lower:
        fallback_languages.append("Spanish")

    fallback_json = {
        "summary": extracted_text[:500].strip(),
        "seniority": fallback_seniority,
        "skills": [keyword for keyword in skills_keywords if keyword in text_lower],
        "languages": fallback_languages,
        "experience": [],
        "education": [],
    }

    agent = await get_tyr_agent("resume", "ResumeParserAgent", RESUME_PARSE_PROMPT)
    if agent is None:
        return {
            "status": True,
            "message": "Resume parsed with deterministic fallback",
            "data": {
                "extractedText": extracted_text,
                "extractedJson": fallback_json,
                "parseStatus": "FALLBACK",
                "parseConfidence": 0.25,
            },
        }

    try:
        user_input = json.dumps(
            {
                "fileName": file_name,
                "resumeText": extracted_text[:RESUME_AI_MAX_INPUT_CHARS],
            },
            ensure_ascii=False,
        )

        raw_response = await asyncio.wait_for(
            asyncio.to_thread(
                lambda: asyncio.run(agent.chat(user_input, save_history=False))
            ),
            timeout=AI_REQUEST_TIMEOUT_SECONDS,
        )
        payload = _parse_agent_json(raw_response)

        summary = str(payload.get("summary") or fallback_json["summary"]).strip()

        seniority = payload.get("seniority") or fallback_json["seniority"]
        seniority = str(seniority).strip().upper() if seniority else None

        skills = [
            str(item).strip()
            for item in (payload.get("skills") or fallback_json["skills"])
            if str(item).strip()
        ]
        languages = [
            str(item).strip()
            for item in (payload.get("languages") or fallback_json["languages"])
            if str(item).strip()
        ]

        return {
            "status": True,
            "message": "Resume parsed successfully",
            "data": {
                "extractedText": extracted_text,
                "extractedJson": {
                    "summary": summary,
                    "seniority": seniority,
                    "skills": skills,
                    "languages": languages,
                    "experience": payload.get("experience") or [],
                    "education": payload.get("education") or [],
                },
                "parseStatus": "COMPLETED",
                "parseConfidence": _normalize_confidence(payload.get("confidence")),
            },
        }
    except Exception as error:
        logger.warning("ai_service_resume_parse_failed error=%s", error)
        return {
            "status": True,
            "message": "Resume parsed with deterministic fallback",
            "data": {
                "extractedText": extracted_text,
                "extractedJson": fallback_json,
                "parseStatus": "FALLBACK",
                "parseConfidence": 0.25,
            },
        }


async def score_job_fit(
    job_context: dict,
    profile_context: dict | None = None,
    resume_context: dict | None = None,
) -> dict:
    agent = await get_tyr_agent("scoring", "JobScoringAgent", SCORING_PROMPT)
    if agent is None:
        return {
            "status": False,
            "message": "AI scoring is unavailable",
            "data": {},
        }

    try:
        raw_job_stack = job_context.get("techStack") or []
        if not isinstance(raw_job_stack, list):
            raw_job_stack = []

        raw_target_roles = (profile_context or {}).get("targetRoles") or []
        if not isinstance(raw_target_roles, list):
            raw_target_roles = []

        raw_preferred_locations = (profile_context or {}).get("preferredLocations") or []
        if not isinstance(raw_preferred_locations, list):
            raw_preferred_locations = []

        raw_must_have = (profile_context or {}).get("mustHaveSkills") or []
        if not isinstance(raw_must_have, list):
            raw_must_have = []

        raw_nice_to_have = (profile_context or {}).get("niceToHaveSkills") or []
        if not isinstance(raw_nice_to_have, list):
            raw_nice_to_have = []

        raw_resume_skills = (resume_context or {}).get("skills") or []
        if not isinstance(raw_resume_skills, list):
            raw_resume_skills = []

        raw_resume_languages = (resume_context or {}).get("languages") or []
        if not isinstance(raw_resume_languages, list):
            raw_resume_languages = []

        compact_job_context = {
            "jobId": job_context.get("jobId"),
            "title": job_context.get("title"),
            "company": job_context.get("company"),
            "location": job_context.get("location"),
            "description": str(job_context.get("description") or "").strip()[
                :AI_SCORE_DESCRIPTION_MAX_CHARS
            ],
            "requirements": str(job_context.get("requirements") or "").strip()[
                :AI_SCORE_REQUIREMENTS_MAX_CHARS
            ],
            "employmentType": job_context.get("employmentType"),
            "seniorityHint": job_context.get("seniorityHint"),
            "remotePolicy": job_context.get("remotePolicy"),
            "techStack": [
                str(item).strip()
                for item in raw_job_stack[:20]
                if str(item).strip()
            ],
            "source": job_context.get("source"),
            "sourceUrl": job_context.get("sourceUrl"),
        }

        compact_profile_context = {
            "objective": str((profile_context or {}).get("objective") or "").strip()[:400],
            "seniority": (profile_context or {}).get("seniority"),
            "targetRoles": [
                str(item).strip()
                for item in raw_target_roles[:10]
                if str(item).strip()
            ],
            "preferredLocations": [
                str(item).strip()
                for item in raw_preferred_locations[:10]
                if str(item).strip()
            ],
            "preferredWorkModel": (profile_context or {}).get("preferredWorkModel"),
            "salaryExpectation": str((profile_context or {}).get("salaryExpectation") or "").strip()[:120],
            "mustHaveSkills": [
                str(item).strip()
                for item in raw_must_have[:20]
                if str(item).strip()
            ],
            "niceToHaveSkills": [
                str(item).strip()
                for item in raw_nice_to_have[:20]
                if str(item).strip()
            ],
        }

        compact_resume_context = {
            "summary": str((resume_context or {}).get("summary") or "").strip()[:AI_SCORE_SUMMARY_MAX_CHARS],
            "seniority": (resume_context or {}).get("seniority"),
            "skills": [
                str(item).strip()
                for item in raw_resume_skills[:30]
                if str(item).strip()
            ],
            "languages": [
                str(item).strip()
                for item in raw_resume_languages[:10]
                if str(item).strip()
            ],
            "parseStatus": (resume_context or {}).get("parseStatus"),
        }

        user_input = json.dumps(
            {
                "job": compact_job_context,
                "profile": compact_profile_context,
                "resume": compact_resume_context,
            },
            ensure_ascii=False,
        )

        raw_response = await asyncio.wait_for(
            asyncio.to_thread(
                lambda: asyncio.run(agent.chat(user_input, save_history=False))
            ),
            timeout=AI_REQUEST_TIMEOUT_SECONDS,
        )
        payload = _parse_agent_json(raw_response)

        ai_score = max(
            0.0,
            min(100.0, float(payload.get("aiScore", payload.get("score")))),
        )

        raw_breakdown = payload.get("breakdown") or {}
        if not isinstance(raw_breakdown, dict):
            raw_breakdown = {}
        breakdown: dict[str, float] = {}

        for key in ("skillsFit", "seniorityFit", "scopeFit", "locationFit"):
            value = raw_breakdown.get(key)
            if value is None:
                continue

            try:
                breakdown[key] = round(max(0.0, min(100.0, float(value))), 2)
            except Exception:
                continue

        return {
            "status": True,
            "message": "AI scoring completed",
            "data": {
                "aiScore": round(ai_score, 2),
                "confidence": _normalize_confidence(payload.get("confidence")),
                "reason": str(payload.get("reason") or "AI scoring completed").strip(),
                "breakdown": breakdown,
            },
        }
    except Exception as error:
        logger.warning("ai_service_scoring_failed error=%s", error)
        return {
            "status": False,
            "message": "Failed to compute AI score",
            "data": {},
        }


async def build_auto_apply_payload(
    job_context: dict,
    profile_context: dict | None = None,
    resume_context: dict | None = None,
) -> dict:
    profile_context = profile_context or {}
    resume_context = resume_context or {}

    fallback_payload = {
        "answers": {
            "whyInterested": (
                f"I am interested in {job_context.get('title') or 'this role'} at "
                f"{job_context.get('company') or 'the company'} because it aligns with my current goals."
            ),
            "highlightExperience": (
                resume_context.get("summary")
                or "My experience is aligned with the main requirements of this role."
            ),
            "salaryExpectation": profile_context.get("salaryExpectation"),
            "workModel": profile_context.get("preferredWorkModel"),
            "additionalNotes": "Prepared for assisted submission with human confirmation.",
        },
        "confidence": 0.4,
        "notes": "Fallback auto-apply payload generated without AI",
    }

    agent = await get_tyr_agent("apply", "AutoApplyAgent", AUTO_APPLY_PROMPT)
    if agent is None:
        return {
            "status": True,
            "message": "Auto-apply payload generated with fallback",
            "data": fallback_payload,
        }

    try:
        user_input = json.dumps(
            {
                "job": job_context,
                "profile": profile_context,
                "resume": resume_context,
            },
            ensure_ascii=False,
        )

        raw_response = await asyncio.wait_for(
            asyncio.to_thread(
                lambda: asyncio.run(agent.chat(user_input, save_history=False))
            ),
            timeout=AI_REQUEST_TIMEOUT_SECONDS,
        )
        payload = _parse_agent_json(raw_response)

        merged_answers = dict(fallback_payload["answers"])
        for key, value in (payload.get("answers") or {}).items():
            normalized = str(value).strip()
            if normalized:
                merged_answers[str(key)] = normalized

        return {
            "status": True,
            "message": "Auto-apply payload generated successfully",
            "data": {
                "answers": merged_answers,
                "confidence": _normalize_confidence(payload.get("confidence")),
                "notes": str(payload.get("notes") or "Auto-apply payload generated by AI").strip(),
            },
        }
    except Exception as error:
        logger.warning("ai_service_auto_apply_failed error=%s", error)
        return {
            "status": True,
            "message": "Auto-apply payload generated with fallback",
            "data": fallback_payload,
        }
