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
from prompts.ai_prompts import RESUME_PARSE_PROMPT, SCORING_PROMPT
from services.integrations.tyr_agent_client import get_tyr_agent
from services.rules.text_normalization import infer_seniority_level, normalize_text


_RESUME_SENIORITY_BY_LEVEL = {
    1: "JUNIOR",
    2: "MID",
    3: "SENIOR",
    4: "LEAD",
}

_RESUME_SENIORITY_LEVEL_BY_NAME = {
    "JUNIOR": 1,
    "MID": 2,
    "SENIOR": 3,
    "LEAD": 4,
    "STAFF": 4,
}


def _first_present(payload: dict, *keys: str):
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return None


def _coerce_str_list(value) -> list[str]:
    if value is None:
        return []

    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    if isinstance(value, str):
        text = value.replace("\r", "\n").replace(";", ",")
        items: list[str] = []
        for line in text.split("\n"):
            for token in line.split(","):
                item = token.strip()
                if item:
                    items.append(item)
        return items

    return []


def _coerce_structured_list(value) -> list[dict]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]

    if isinstance(value, dict):
        for key in ("items", "list", "entries"):
            nested = value.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]

    return []


def _canonical_resume_seniority(value) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None

    level = infer_seniority_level(text)
    if level in _RESUME_SENIORITY_BY_LEVEL:
        return _RESUME_SENIORITY_BY_LEVEL[level]

    upper = text.upper()
    if upper in _RESUME_SENIORITY_LEVEL_BY_NAME:
        return upper

    return None


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


def _sync_agent_chat(agent, user_input: str) -> str:
    agent_model = getattr(agent, "agent_model", None)
    prompt_build = getattr(agent, "prompt_build", None)

    if agent_model is not None and prompt_build is not None and hasattr(agent_model, "generate"):
        output = agent_model.generate(
            prompt_build,
            user_input,
            None,
            getattr(agent, "history", None),
            bool(getattr(agent, "use_history", False)),
        )
        return str(output or "")

    chat_fn = getattr(agent, "chat", None)
    if callable(chat_fn):
        output = asyncio.run(chat_fn(user_input, save_history=False))
        return str(output or "")

    return ""


def _normalize_confidence(value):
    try:
        confidence = float(value)
        if confidence > 1.0 and confidence <= 100.0:
            confidence = confidence / 100.0
        return max(0.0, min(1.0, confidence))
    except Exception:
        return None


def _derive_resume_parse_confidence(parsed_resume: dict) -> float:
    score = 0.10
    if str(parsed_resume.get("summary") or "").strip():
        score += 0.30
    if parsed_resume.get("seniority"):
        score += 0.15
    if parsed_resume.get("skills"):
        score += 0.25
    if parsed_resume.get("languages"):
        score += 0.05
    if parsed_resume.get("experience"):
        score += 0.10
    if parsed_resume.get("education"):
        score += 0.05

    return max(0.15, min(0.95, round(score, 2)))


def _normalize_resume_payload(payload: dict, fallback_json: dict) -> tuple[dict, float | None, bool]:
    if not isinstance(payload, dict):
        payload = {}

    summary_raw = str(
        _first_present(payload, "summary", "resumo", "about", "profileSummary") or ""
    ).strip()

    seniority_raw = _first_present(payload, "seniority", "senioridade", "nivel")
    seniority = _canonical_resume_seniority(seniority_raw)
    fallback_seniority = str(fallback_json.get("seniority") or "").strip().upper() or None

    skills = _coerce_str_list(
        _first_present(payload, "skills", "habilidades", "competencias", "stack")
    )
    languages = _coerce_str_list(
        _first_present(payload, "languages", "idiomas", "linguas")
    )

    experience = _coerce_structured_list(
        _first_present(
            payload,
            "experience",
            "experiencia",
            "experiencias",
            "professionalExperience",
            "workExperience",
        )
    )

    education = _coerce_structured_list(
        _first_present(
            payload,
            "education",
            "educacao",
            "formacao",
            "academicBackground",
        )
    )

    has_meaningful_payload = bool(
        summary_raw or seniority or skills or languages or experience or education
    )

    if not has_meaningful_payload:
        return fallback_json, 0.25, False

    ai_level = _RESUME_SENIORITY_LEVEL_BY_NAME.get(seniority or "")
    fallback_level = _RESUME_SENIORITY_LEVEL_BY_NAME.get(fallback_seniority or "")
    if ai_level and fallback_level and (ai_level - fallback_level) >= 2:
        seniority = fallback_seniority

    normalized_resume = {
        "summary": summary_raw or fallback_json["summary"],
        "seniority": seniority or fallback_seniority,
        "skills": skills or fallback_json["skills"],
        "languages": languages or fallback_json["languages"],
        "experience": experience,
        "education": education,
    }

    confidence = _normalize_confidence(payload.get("confidence"))
    if confidence is None:
        confidence = _derive_resume_parse_confidence(normalized_resume)

    return normalized_resume, confidence, True


async def parse_resume_pdf(file_bytes: bytes, file_name: str) -> dict:
    extracted_text = extract_text_from_pdf_bytes(file_bytes)
    if len(extracted_text) < RESUME_MIN_EXTRACTED_TEXT_CHARS:
        short_text = extracted_text.strip()
        low_text_fallback_json = {
            "summary": short_text[:500],
            "seniority": None,
            "skills": [],
            "languages": [],
            "experience": [],
            "education": [],
        }

        return {
            "status": True,
            "message": "Resume parsed with low-text fallback",
            "data": {
                "extractedText": extracted_text,
                "extractedJson": low_text_fallback_json,
                "parseStatus": "FALLBACK",
                "parseConfidence": 0.1,
            },
        }

    text_lower = normalize_text(extracted_text)
    fallback_seniority_level = infer_seniority_level(extracted_text)
    fallback_seniority = _RESUME_SENIORITY_BY_LEVEL.get(fallback_seniority_level)

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
        logger.warning("ai_service_resume_agent_unavailable file_name=%s", file_name)
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
            asyncio.to_thread(_sync_agent_chat, agent, user_input),
            timeout=AI_REQUEST_TIMEOUT_SECONDS,
        )
        payload = _parse_agent_json(raw_response)

        normalized_resume, parse_confidence, has_ai_payload = _normalize_resume_payload(
            payload,
            fallback_json,
        )

        if not has_ai_payload:
            logger.warning(
                "ai_service_resume_empty_ai_payload file_name=%s payload_keys=%s",
                file_name,
                sorted(payload.keys()),
            )
            return {
                "status": True,
                "message": "Resume parsed with deterministic fallback",
                "data": {
                    "extractedText": extracted_text,
                    "extractedJson": normalized_resume,
                    "parseStatus": "FALLBACK",
                    "parseConfidence": parse_confidence,
                },
            }

        return {
            "status": True,
            "message": "Resume parsed successfully",
            "data": {
                "extractedText": extracted_text,
                "extractedJson": normalized_resume,
                "parseStatus": "COMPLETED",
                "parseConfidence": parse_confidence,
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
        profile_context = profile_context or {}
        resume_context = resume_context or {}

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
                for item in (job_context.get("techStack") or [])[:20]
                if str(item).strip()
            ],
            "source": job_context.get("source"),
            "sourceUrl": job_context.get("sourceUrl"),
        }

        compact_profile_context = {
            "objective": str(profile_context.get("objective") or "").strip()[:400],
            "seniority": profile_context.get("seniority"),
            "targetRoles": [
                str(item).strip()
                for item in (profile_context.get("targetRoles") or [])[:10]
                if str(item).strip()
            ],
            "preferredLocations": [
                str(item).strip()
                for item in (profile_context.get("preferredLocations") or [])[:10]
                if str(item).strip()
            ],
            "preferredWorkModel": profile_context.get("preferredWorkModel"),
            "salaryExpectation": str(profile_context.get("salaryExpectation") or "").strip()[:120],
            "mustHaveSkills": [
                str(item).strip()
                for item in (profile_context.get("mustHaveSkills") or [])[:20]
                if str(item).strip()
            ],
            "niceToHaveSkills": [
                str(item).strip()
                for item in (profile_context.get("niceToHaveSkills") or [])[:20]
                if str(item).strip()
            ],
        }

        compact_resume_context = {
            "summary": str(resume_context.get("summary") or "").strip()[:AI_SCORE_SUMMARY_MAX_CHARS],
            "seniority": resume_context.get("seniority"),
            "skills": [
                str(item).strip()
                for item in (resume_context.get("skills") or [])[:30]
                if str(item).strip()
            ],
            "languages": [
                str(item).strip()
                for item in (resume_context.get("languages") or [])[:10]
                if str(item).strip()
            ],
            "parseStatus": resume_context.get("parseStatus"),
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
            asyncio.to_thread(_sync_agent_chat, agent, user_input),
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

