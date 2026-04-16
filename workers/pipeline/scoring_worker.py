import asyncio
import hashlib
import json
import uuid

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import (
    AI_DETERMINISTIC_WEIGHT,
    AI_MAX_EFFECTIVE_WEIGHT,
    AI_MAX_CALLS_PER_RUN,
    AI_MIN_CONFIDENCE,
    AI_MIN_CONTEXT_QUALITY,
    AI_SCORING_CACHE_ENABLED,
    AI_SCORING_ENABLED,
    AI_SCORING_MIN_DETERMINISTIC_SCORE,
    AI_SCORING_MIN_ROLE_MATCH,
    AI_SCORING_MIN_SKILL_OVERLAP,
    AI_SCORING_WEIGHT,
    DIGEST_EMAIL_QUEUE,
    PIPELINE_EVENT_DEDUPE_KEY_PREFIX,
    PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
    PIPELINE_SCORING_FAILED_KEY_PREFIX,
    PIPELINE_LAST_RUN_TTL_SECONDS,
    PIPELINE_SCORING_DIGEST_TRIGGER_KEY_PREFIX,
    PIPELINE_SCORING_PROGRESS_KEY_PREFIX,
    PIPELINE_RUN_AI_CALLS_KEY_PREFIX,
    SCORING_JOBS_QUEUE,
    SHORTLIST_APPLY_QUEUE,
)
from core.logger.logger import logger
from core.postgresql.postgresql import postgresql
from core.rabbitmq.rabbitmq import rabbitmq
from core.redis.redis import redis_cache
from services.ai import ai_service
from services.cache import cache_service
from services.messaging import messaging_service
from services.rules import pipeline_state_machine, scoring_policy

DEFAULT_SCORE_WEIGHTS = {
    "role_weight": 0.35,
    "salary_weight": 0.25,
    "location_weight": 0.20,
    "seniority_weight": 0.20,
}


def _event_dedupe_key(event_id: str) -> str:
    return f"{PIPELINE_EVENT_DEDUPE_KEY_PREFIX}:{event_id}"


def _scoring_progress_key(run_id: str) -> str:
    return f"{PIPELINE_SCORING_PROGRESS_KEY_PREFIX}:{run_id}"


def _scoring_failed_key(run_id: str) -> str:
    return f"{PIPELINE_SCORING_FAILED_KEY_PREFIX}:{run_id}"


def _scoring_digest_trigger_key(run_id: str) -> str:
    return f"{PIPELINE_SCORING_DIGEST_TRIGGER_KEY_PREFIX}:{run_id}"


def _run_ai_calls_key(run_id: str, user_id: int) -> str:
    return f"{PIPELINE_RUN_AI_CALLS_KEY_PREFIX}:{run_id}:{user_id}"


async def _should_publish_digest(
    run_id: str,
    user_id: int,
    total_jobs: int,
    scoring_succeeded: bool,
) -> bool:
    redis_client = redis_cache.redis
    if redis_client is None:
        logger.error("scoring_worker_redis_not_connected run_id=%s user_id=%s", run_id, user_id)
        return False

    try:
        expected_jobs = max(1, int(total_jobs or 1))
        progress_key = _scoring_progress_key(run_id)
        failed_key = _scoring_failed_key(run_id)
        counter_key = progress_key if scoring_succeeded else failed_key

        updated_count = int(await redis_client.incr(counter_key))
        if updated_count == 1:
            await redis_client.expire(counter_key, PIPELINE_LAST_RUN_TTL_SECONDS)

        processed_raw, failed_raw = await redis_client.mget(progress_key, failed_key)
        processed_count = int(processed_raw or 0)
        failed_count = int(failed_raw or 0)
        finished_count = processed_count + failed_count

        logger.info(
            "scoring_worker_progress run_id=%s user_id=%s processed=%s failed=%s finished=%s expected=%s success=%s",
            run_id,
            user_id,
            processed_count,
            failed_count,
            finished_count,
            expected_jobs,
            scoring_succeeded,
        )

        if finished_count < expected_jobs:
            return False

        trigger_key = _scoring_digest_trigger_key(run_id)
        trigger_value = f"{run_id}:{user_id}"
        return await cache_service.acquire_lock(
            trigger_key,
            trigger_value,
            PIPELINE_LAST_RUN_TTL_SECONDS,
            redis_client,
            fail_open=False,
        )
    except Exception as e:
        logger.exception(e)
        return False


def _normalize_text(value) -> str:
    return str(value or "").strip().lower()


def _tokenize_text(*values) -> set[str]:
    tokens: set[str] = set()

    for value in values:
        text = _normalize_text(value)
        if not text:
            continue

        cleaned = "".join(
            character if character.isalnum() or character in {"+", "#", "."} else " "
            for character in text
        )

        for token in cleaned.split():
            if len(token) >= 2:
                tokens.add(token)

    return tokens


def _infer_seniority_level(*values) -> int | None:
    text = " ".join(_normalize_text(value) for value in values if value)
    if not text:
        return None

    if any(token in text for token in ["staff", "principal", "architect"]):
        return 4
    if any(token in text for token in ["lead", "manager", "head"]):
        return 4
    if any(token in text for token in ["senior", " sr ", " sr."]):
        return 3
    if any(token in text for token in ["mid", "middle", "pleno"]):
        return 2
    if any(token in text for token in ["junior", "entry", "intern", "trainee"]):
        return 1

    return None


def _signal_from_context(
    job_context: dict,
    profile_context: dict,
    resume_context: dict,
) -> tuple[dict[str, float], dict]:
    title = job_context.get("title") or ""
    description = job_context.get("description") or ""
    requirements = job_context.get("requirements") or ""
    location = _normalize_text(job_context.get("location"))
    remote_policy = _normalize_text(job_context.get("remotePolicy"))
    tech_stack = _list_from_value(job_context.get("techStack"))

    title_tokens = _tokenize_text(title)
    job_tokens = _tokenize_text(title, description, requirements, " ".join(tech_stack), location)

    target_roles = _list_from_value(profile_context.get("targetRoles"))
    must_have_skills = _list_from_value(profile_context.get("mustHaveSkills"))
    nice_to_have_skills = _list_from_value(profile_context.get("niceToHaveSkills"))
    resume_skills = _list_from_value(resume_context.get("skills"))

    role_match = 0.0
    if target_roles:
        for role in target_roles:
            role_tokens = _tokenize_text(role)
            if not role_tokens:
                continue

            overlap = len(title_tokens & role_tokens) / len(role_tokens)
            role_match = max(role_match, overlap)
    else:
        role_match = (
            1.0 if any(token in title_tokens for token in {"engineer", "developer", "backend"}) else 0.55
        )

    candidate_skills = (must_have_skills or resume_skills)[:20]
    skill_hits = 0
    for skill in candidate_skills:
        skill_tokens = _tokenize_text(skill)
        if not skill_tokens:
            continue

        if skill_tokens & job_tokens:
            skill_hits += 1

    skill_overlap = (
        (skill_hits / len(candidate_skills)) if candidate_skills else 0.55
    )

    nice_hits = 0
    for skill in nice_to_have_skills[:20]:
        skill_tokens = _tokenize_text(skill)
        if skill_tokens & job_tokens:
            nice_hits += 1

    nice_overlap = (
        (nice_hits / len(nice_to_have_skills[:20])) if nice_to_have_skills else 0.5
    )

    role_signal = max(0.35, min(1.0, 0.45 * role_match + 0.45 * skill_overlap + 0.10 * nice_overlap))

    candidate_seniority = _infer_seniority_level(
        profile_context.get("seniority"),
        resume_context.get("seniority"),
        profile_context.get("objective"),
    )
    job_seniority = _infer_seniority_level(
        job_context.get("seniorityHint"),
        title,
        description,
        requirements,
    )

    if candidate_seniority and job_seniority:
        seniority_gap = abs(job_seniority - candidate_seniority)
        seniority_signal = 1.0 if seniority_gap == 0 else 0.74 if seniority_gap == 1 else 0.45
        if job_seniority < candidate_seniority:
            seniority_signal = max(0.35, seniority_signal - 0.08)
    elif candidate_seniority or job_seniority:
        seniority_signal = 0.68
    else:
        seniority_signal = 0.75

    has_salary_expectation = bool((profile_context.get("salaryExpectation") or "").strip())
    if has_salary_expectation and candidate_seniority and job_seniority:
        if job_seniority >= candidate_seniority:
            salary_signal = 0.90
        elif candidate_seniority - job_seniority == 1:
            salary_signal = 0.62
        else:
            salary_signal = 0.42
    else:
        salary_signal = max(0.55, min(0.92, 0.60 + 0.40 * skill_overlap))

    preferred_work_model = _normalize_text(profile_context.get("preferredWorkModel"))
    preferred_locations = _list_from_value(profile_context.get("preferredLocations"))
    is_job_remote = "remote" in location or "remote" in remote_policy
    is_job_hybrid = "hybrid" in location or "hybrid" in remote_policy

    location_signal = 0.65
    if preferred_work_model:
        if "remote" in preferred_work_model:
            location_signal = 1.0 if is_job_remote else 0.38
        elif "hybrid" in preferred_work_model:
            if is_job_hybrid:
                location_signal = 1.0
            elif is_job_remote:
                location_signal = 0.78
            else:
                location_signal = 0.52
        elif "onsite" in preferred_work_model or "on-site" in preferred_work_model:
            location_signal = 0.95 if not (is_job_remote or is_job_hybrid) else 0.45

    if preferred_locations:
        preferred_locations_normalized = [_normalize_text(item) for item in preferred_locations]
        location_match = any(item and item in location for item in preferred_locations_normalized)
        accepts_remote = any("remote" in item for item in preferred_locations_normalized)

        if location_match or (accepts_remote and is_job_remote):
            location_signal = min(1.0, location_signal + 0.20)
        elif not is_job_remote:
            location_signal = min(location_signal, 0.50)

    signals = {
        "role_weight": round(max(0.0, min(1.0, role_signal)), 4),
        "salary_weight": round(max(0.0, min(1.0, salary_signal)), 4),
        "location_weight": round(max(0.0, min(1.0, location_signal)), 4),
        "seniority_weight": round(max(0.0, min(1.0, seniority_signal)), 4),
    }

    details = {
        "roleMatch": round(role_match, 2),
        "skillHits": skill_hits,
        "skillTotal": len(candidate_skills),
        "jobSeniority": job_seniority,
        "candidateSeniority": candidate_seniority,
    }

    return signals, details


def _reason_from_signals(signals: dict[str, float], details: dict) -> str:
    return (
        "signals="
        f"role:{signals['role_weight']:.2f},"
        f"salary:{signals['salary_weight']:.2f},"
        f"location:{signals['location_weight']:.2f},"
        f"seniority:{signals['seniority_weight']:.2f}"
        f" | role_match:{float(details.get('roleMatch') or 0):.2f}"
        f" skills:{int(details.get('skillHits') or 0)}/{int(details.get('skillTotal') or 0)}"
        f" seniority:{details.get('candidateSeniority')}->{details.get('jobSeniority')}"
    )


def _compute_score(weights: dict[str, float], signals: dict[str, float]) -> float:
    score = 0.0
    for key, weight in weights.items():
        score += max(0.0, float(weight)) * max(0.0, float(signals.get(key, 0.0))) * 100
    return scoring_policy.clamp_score(score)


def _build_ai_context_hash(job_context: dict, profile_context: dict, resume_context: dict) -> str:
    payload = {
        "job": {
            "title": job_context.get("title"),
            "company": job_context.get("company"),
            "location": job_context.get("location"),
            "description": job_context.get("description"),
            "requirements": job_context.get("requirements"),
            "employmentType": job_context.get("employmentType"),
            "seniorityHint": job_context.get("seniorityHint"),
            "remotePolicy": job_context.get("remotePolicy"),
            "techStack": _list_from_value(job_context.get("techStack")),
            "source": job_context.get("source"),
            "sourceUrl": job_context.get("sourceUrl"),
        },
        "profile": {
            "objective": profile_context.get("objective"),
            "seniority": profile_context.get("seniority"),
            "targetRoles": _list_from_value(profile_context.get("targetRoles")),
            "preferredLocations": _list_from_value(profile_context.get("preferredLocations")),
            "preferredWorkModel": profile_context.get("preferredWorkModel"),
            "salaryExpectation": profile_context.get("salaryExpectation"),
            "mustHaveSkills": _list_from_value(profile_context.get("mustHaveSkills")),
            "niceToHaveSkills": _list_from_value(profile_context.get("niceToHaveSkills")),
        },
        "resume": {
            "summary": resume_context.get("summary"),
            "seniority": resume_context.get("seniority"),
            "skills": _list_from_value(resume_context.get("skills")),
            "languages": _list_from_value(resume_context.get("languages")),
            "experience": resume_context.get("experience") or [],
            "education": resume_context.get("education") or [],
            "parseStatus": resume_context.get("parseStatus"),
        },
    }

    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _context_quality(job_context: dict, profile_context: dict, resume_context: dict) -> float:
    job_quality = (
        0.15 * bool((job_context.get("title") or "").strip())
        + 0.10 * bool((job_context.get("company") or "").strip())
        + 0.10 * bool((job_context.get("location") or "").strip())
        + 0.35 * bool((job_context.get("description") or "").strip())
        + 0.20 * bool((job_context.get("requirements") or "").strip())
        + 0.10 * bool(_list_from_value(job_context.get("techStack")))
    )

    profile_quality = (
        0.20 * bool((profile_context.get("objective") or "").strip())
        + 0.20 * bool((profile_context.get("seniority") or "").strip())
        + 0.20 * bool(_list_from_value(profile_context.get("mustHaveSkills")))
        + 0.15 * bool(_list_from_value(profile_context.get("targetRoles")))
        + 0.10 * bool((profile_context.get("salaryExpectation") or "").strip())
        + 0.15 * bool((profile_context.get("preferredWorkModel") or "").strip())
    )

    resume_quality = (
        0.30 * bool((resume_context.get("summary") or "").strip())
        + 0.35 * bool(_list_from_value(resume_context.get("skills")))
        + 0.20 * bool(resume_context.get("experience") or [])
        + 0.15
        * (
            1.0
            if str(resume_context.get("parseStatus") or "").strip().upper()
            == "COMPLETED"
            else 0.5
            if str(resume_context.get("parseStatus") or "").strip()
            else 0.0
        )
    )

    combined = 0.60 * job_quality + 0.15 * profile_quality + 0.25 * resume_quality
    return max(0.0, min(1.0, round(combined, 4)))


def _compose_final_score(
    deterministic_score: float,
    ai_score: float | None,
    ai_confidence: float | None,
    context_quality: float,
) -> tuple[float, float, bool]:
    if ai_score is None:
        return scoring_policy.clamp_score(deterministic_score), 0.0, False

    confidence = max(0.0, min(1.0, float(ai_confidence or 0.0)))
    quality = max(0.0, min(1.0, float(context_quality or 0.0)))

    if confidence < AI_MIN_CONFIDENCE or quality < AI_MIN_CONTEXT_QUALITY:
        return scoring_policy.clamp_score(deterministic_score), 0.0, False

    deterministic_weight = max(0.0, float(AI_DETERMINISTIC_WEIGHT))
    ai_weight = max(0.0, float(AI_SCORING_WEIGHT))
    total_weight = deterministic_weight + ai_weight
    if total_weight <= 0:
        return scoring_policy.clamp_score(deterministic_score), 0.0, False

    base_ai_weight = ai_weight / total_weight
    effective_ai_weight = min(
        float(AI_MAX_EFFECTIVE_WEIGHT),
        base_ai_weight * confidence * quality,
    )

    if effective_ai_weight <= 0:
        return scoring_policy.clamp_score(deterministic_score), 0.0, False

    final_score = (
        (1 - effective_ai_weight) * deterministic_score
        + effective_ai_weight * ai_score
    )

    return scoring_policy.clamp_score(final_score), round(effective_ai_weight, 4), True


def _list_from_value(value) -> list[str]:
    if value is None:
        return []

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []

    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    return []


def _dict_from_value(value) -> dict:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return {}

    return value if isinstance(value, dict) else {}


async def _get_ai_context(conn, user_id: int) -> tuple[dict, dict]:
    profile_row = await conn.fetchrow(
        """
        SELECT
            objective,
            seniority,
            target_roles,
            preferred_locations,
            preferred_work_model,
            salary_expectation,
            must_have_skills,
            nice_to_have_skills
        FROM user_profiles
        WHERE user_id = $1
        """,
        user_id,
    )

    resume_row = await conn.fetchrow(
        """
        SELECT
            extracted_json,
            parse_status,
            parse_confidence
        FROM user_resumes
        WHERE user_id = $1 AND is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
        """,
        user_id,
    )

    profile_context = {}
    if profile_row:
        profile_context = {
            "objective": profile_row["objective"],
            "seniority": profile_row["seniority"],
            "targetRoles": _list_from_value(profile_row["target_roles"]),
            "preferredLocations": _list_from_value(profile_row["preferred_locations"]),
            "preferredWorkModel": profile_row["preferred_work_model"],
            "salaryExpectation": profile_row["salary_expectation"],
            "mustHaveSkills": _list_from_value(profile_row["must_have_skills"]),
            "niceToHaveSkills": _list_from_value(profile_row["nice_to_have_skills"]),
        }

    resume_context = {}
    if resume_row:
        resume_context = _dict_from_value(resume_row["extracted_json"])
        resume_context["parseStatus"] = resume_row["parse_status"]
        parse_confidence = resume_row["parse_confidence"]
        resume_context["parseConfidence"] = (
            float(parse_confidence) if parse_confidence is not None else None
        )

    return profile_context, resume_context


async def _get_weights(conn, user_id: int) -> dict[str, float]:
    row = await conn.fetchrow(
        """
        SELECT
            role_weight,
            salary_weight,
            location_weight,
            seniority_weight
        FROM score_weights
        WHERE user_id = $1
        """,
        user_id,
    )

    if not row:
        return DEFAULT_SCORE_WEIGHTS

    return {
        "role_weight": float(row["role_weight"]),
        "salary_weight": float(row["salary_weight"]),
        "location_weight": float(row["location_weight"]),
        "seniority_weight": float(row["seniority_weight"]),
    }


async def process_scoring_event(message: AbstractIncomingMessage) -> None:
    async with message.process():
        payload = json.loads(message.body.decode())
        event_id = payload.get("event_id")
        run_id = payload.get("run_id")
        user_id = payload.get("user_id")
        job_id = payload.get("job_id")
        sequence = int(payload.get("sequence") or 1)
        total_jobs = int(payload.get("total_jobs") or 1)

        if not event_id or not run_id or not user_id or not job_id:
            logger.error("scoring_worker_invalid_event payload=%s", payload)
            return

        dedupe_key = _event_dedupe_key(str(event_id))
        is_new_event = await cache_service.acquire_lock(
            dedupe_key,
            str(event_id),
            PIPELINE_EVENT_DEDUPE_TTL_SECONDS,
            redis_cache.redis,
            fail_open=True,
        )

        if not is_new_event:
            logger.info("scoring_worker_duplicate_event event_id=%s", event_id)
            return

        async with postgresql.pool.acquire() as conn:
            job_row = await conn.fetchrow(
                """
                SELECT
                    id,
                    user_id,
                    title,
                    company,
                    location,
                    description,
                    requirements,
                    employment_type,
                    seniority_hint,
                    remote_policy,
                    tech_stack,
                    source,
                    source_url,
                    status
                FROM jobs
                WHERE id = $1 AND user_id = $2
                """,
                job_id,
                user_id,
            )

            if not job_row:
                logger.error(
                    "scoring_worker_job_not_found run_id=%s user_id=%s job_id=%s",
                    run_id,
                    user_id,
                    job_id,
                )
                return

            weights = await _get_weights(conn, int(user_id))
            ai_score = None
            ai_confidence = None
            ai_reason = None
            ai_breakdown = None
            ai_context_hash = None
            ai_skipped_reason = None
            ai_cache_hit = False
            ai_calls_count = 0
            effective_ai_weight = 0.0
            ai_used = False

            job_context = {
                "jobId": int(job_row["id"]),
                "title": job_row["title"],
                "company": job_row["company"],
                "location": job_row["location"],
                "description": job_row["description"],
                "requirements": job_row["requirements"],
                "employmentType": job_row["employment_type"],
                "seniorityHint": job_row["seniority_hint"],
                "remotePolicy": job_row["remote_policy"],
                "techStack": _list_from_value(job_row["tech_stack"]),
                "source": job_row["source"],
                "sourceUrl": job_row["source_url"],
            }
            profile_context, resume_context = await _get_ai_context(conn, int(user_id))
            signals, signal_details = _signal_from_context(
                job_context,
                profile_context,
                resume_context,
            )
            deterministic_score = _compute_score(weights, signals)
            context_quality = _context_quality(
                job_context,
                profile_context,
                resume_context,
            )
            role_match = float(signal_details.get("roleMatch") or 0.0)
            skill_hits = int(signal_details.get("skillHits") or 0)
            skill_total = int(signal_details.get("skillTotal") or 0)
            skill_overlap = (skill_hits / skill_total) if skill_total > 0 else 0.0
            ai_context_hash = _build_ai_context_hash(
                job_context,
                profile_context,
                resume_context,
            )

            existing_score_row = await conn.fetchrow(
                """
                SELECT
                    ai_context_hash,
                    ai_score,
                    ai_confidence,
                    ai_reason,
                    ai_breakdown
                FROM job_scores
                WHERE user_id = $1 AND job_id = $2
                """,
                int(user_id),
                int(job_id),
            )

            if AI_SCORING_ENABLED:
                if (
                    AI_SCORING_CACHE_ENABLED
                    and existing_score_row
                    and existing_score_row["ai_context_hash"] == ai_context_hash
                    and existing_score_row["ai_score"] is not None
                ):
                    ai_score = scoring_policy.clamp_score(float(existing_score_row["ai_score"]))
                    ai_confidence = (
                        float(existing_score_row["ai_confidence"])
                        if existing_score_row["ai_confidence"] is not None
                        else None
                    )
                    ai_reason = existing_score_row["ai_reason"]
                    ai_breakdown = _dict_from_value(existing_score_row["ai_breakdown"])
                    ai_cache_hit = True
                    ai_skipped_reason = "cache_hit"
                elif deterministic_score < float(AI_SCORING_MIN_DETERMINISTIC_SCORE):
                    ai_skipped_reason = "below_deterministic_threshold"
                elif role_match < float(AI_SCORING_MIN_ROLE_MATCH):
                    ai_skipped_reason = "low_role_match"
                elif skill_total > 0 and skill_overlap < float(AI_SCORING_MIN_SKILL_OVERLAP):
                    ai_skipped_reason = "low_skill_overlap"
                elif context_quality < float(AI_MIN_CONTEXT_QUALITY):
                    ai_skipped_reason = "low_context_quality"
                else:
                    ai_call_allowed = True
                    redis_client = redis_cache.redis
                    if redis_client is not None:
                        ai_calls_key = _run_ai_calls_key(str(run_id), int(user_id))
                        ai_calls_count = int(await redis_client.incr(ai_calls_key))
                        if ai_calls_count == 1:
                            await redis_client.expire(ai_calls_key, PIPELINE_LAST_RUN_TTL_SECONDS)

                        if ai_calls_count > max(1, int(AI_MAX_CALLS_PER_RUN)):
                            ai_call_allowed = False
                            ai_skipped_reason = "run_ai_cap_reached"

                    if ai_call_allowed:
                        ai_response = await ai_service.score_job_fit(
                            job_context=job_context,
                            profile_context=profile_context,
                            resume_context=resume_context,
                        )

                        if ai_response.get("status"):
                            ai_payload = ai_response.get("data", {})

                            try:
                                ai_score = scoring_policy.clamp_score(
                                    float(ai_payload.get("aiScore"))
                                )
                            except (TypeError, ValueError):
                                ai_score = None

                            try:
                                raw_confidence = ai_payload.get("confidence")
                                ai_confidence = (
                                    max(0.0, min(1.0, float(raw_confidence)))
                                    if raw_confidence is not None
                                    else None
                                )
                            except (TypeError, ValueError):
                                ai_confidence = None

                            ai_reason = ai_payload.get("reason")
                            ai_breakdown = _dict_from_value(ai_payload.get("breakdown"))
                            ai_skipped_reason = None
                        else:
                            ai_reason = ai_response.get("message")
                            ai_skipped_reason = "ai_scoring_failed"
            else:
                ai_skipped_reason = "ai_disabled"

            final_score, effective_ai_weight, ai_used = _compose_final_score(
                deterministic_score,
                ai_score,
                ai_confidence,
                context_quality,
            )
            score = final_score
            bucket = scoring_policy.bucket_from_score(final_score)
            reason = _reason_from_signals(signals, signal_details)

            deterministic_score_rounded = round(deterministic_score, 2)
            ai_score_rounded = round(ai_score, 2) if ai_score is not None else None
            ai_confidence_rounded = (
                round(ai_confidence, 2) if ai_confidence is not None else None
            )
            final_score_rounded = round(final_score, 2)
            context_quality_rounded = round(context_quality, 2)
            ai_delta = (
                round(abs(ai_score - deterministic_score), 2)
                if ai_score is not None
                else None
            )

            await conn.execute(
                """
                INSERT INTO job_scores (
                    user_id,
                    job_id,
                    score,
                    deterministic_score,
                    ai_score,
                    ai_confidence,
                    final_score,
                    bucket,
                    reason,
                    ai_reason,
                    ai_breakdown,
                    ai_context_hash,
                    ai_skipped_reason
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
                ON CONFLICT (user_id, job_id)
                DO UPDATE SET
                    score = EXCLUDED.score,
                    deterministic_score = EXCLUDED.deterministic_score,
                    ai_score = EXCLUDED.ai_score,
                    ai_confidence = EXCLUDED.ai_confidence,
                    final_score = EXCLUDED.final_score,
                    bucket = EXCLUDED.bucket,
                    reason = EXCLUDED.reason,
                    ai_reason = EXCLUDED.ai_reason,
                    ai_breakdown = EXCLUDED.ai_breakdown,
                    ai_context_hash = EXCLUDED.ai_context_hash,
                    ai_skipped_reason = EXCLUDED.ai_skipped_reason,
                    updated_at = NOW()
                """,
                user_id,
                job_id,
                final_score_rounded,
                deterministic_score_rounded,
                ai_score_rounded,
                ai_confidence_rounded,
                final_score_rounded,
                bucket,
                reason,
                ai_reason,
                json.dumps(ai_breakdown) if ai_breakdown else None,
                ai_context_hash,
                ai_skipped_reason,
            )

            current_status = (job_row["status"] or "INGESTED").strip().upper()
            status_after_scoring = current_status
            if pipeline_state_machine.can_transition(current_status, "SCORED"):
                status_after_scoring = "SCORED"
                await conn.execute(
                    "UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2 AND user_id = $3",
                    status_after_scoring,
                    job_id,
                    user_id,
                )

            if bucket == "A" and pipeline_state_machine.can_transition(
                status_after_scoring,
                "APPLY_READY",
            ):
                await conn.execute(
                    "UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2 AND user_id = $3",
                    "APPLY_READY",
                    job_id,
                    user_id,
                )

        if bucket == "A":
            await messaging_service.publish(
                SHORTLIST_APPLY_QUEUE,
                {
                    "event_id": str(uuid.uuid4()),
                    "event_version": 1,
                    "run_id": run_id,
                    "user_id": user_id,
                    "job_id": job_id,
                    "score": round(score, 2),
                    "bucket": bucket,
                    "sequence": sequence,
                    "total_jobs": total_jobs,
                    "retry_count": 0,
                },
                rabbitmq.channel,
            )

        if await _should_publish_digest(
            str(run_id),
            int(user_id),
            total_jobs,
            scoring_succeeded=True,
        ):
            await messaging_service.publish(
                DIGEST_EMAIL_QUEUE,
                {
                    "event_id": str(uuid.uuid4()),
                    "event_version": 1,
                    "source": "pipeline",
                    "run_id": run_id,
                    "user_id": user_id,
                },
                rabbitmq.channel,
            )

        logger.info(
            "scoring_worker_processed run_id=%s user_id=%s job_id=%s final_score=%.2f deterministic_score=%.2f ai_score=%s ai_confidence=%s context_quality=%.2f role_match=%.2f skill_overlap=%.2f effective_ai_weight=%.4f ai_used=%s ai_cache_hit=%s ai_calls_count=%s ai_skipped_reason=%s ai_delta=%s bucket=%s",
            run_id,
            user_id,
            job_id,
            score,
            deterministic_score_rounded,
            ai_score_rounded,
            ai_confidence_rounded,
            context_quality_rounded,
            round(role_match, 2),
            round(skill_overlap, 2),
            effective_ai_weight,
            ai_used,
            ai_cache_hit,
            ai_calls_count,
            ai_skipped_reason,
            ai_delta,
            bucket,
        )


async def run() -> None:
    await postgresql.connect()
    await redis_cache.connect()
    await rabbitmq.connect()

    assert rabbitmq.channel is not None

    await rabbitmq.channel.set_qos(prefetch_count=1)
    queue = await rabbitmq.channel.declare_queue(SCORING_JOBS_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(SHORTLIST_APPLY_QUEUE, durable=True)
    await rabbitmq.channel.declare_queue(DIGEST_EMAIL_QUEUE, durable=True)
    await queue.consume(process_scoring_event)

    try:
        await asyncio.Future()
    finally:
        await rabbitmq.disconnect()
        await redis_cache.disconnect()
        await postgresql.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
