import hashlib
import json

from core.config.config import (
    AI_DETERMINISTIC_WEIGHT,
    AI_MAX_EFFECTIVE_WEIGHT,
    AI_MIN_CONFIDENCE,
    AI_MIN_CONTEXT_QUALITY,
    AI_SCORING_WEIGHT,
)
from core.utils.json_utils import ensure_str_list
from services.rules import scoring_policy
from services.rules.text_normalization import (
    ABOVE_LEVEL_TITLE_TOKENS,
    HARD_ABOVE_LEVEL_TITLE_TOKENS,
    infer_seniority_level,
    location_signals,
    tokenize_text,
)


AI_PREFILTER_REASON_CODES = (
    "below_deterministic_threshold",
    "low_role_match",
    "low_skill_overlap",
    "low_work_model_signal",
    "low_location_signal",
    "high_seniority_gap",
    "title_above_candidate_level",
)


def signal_from_context(
    job_context: dict,
    profile_context: dict,
    resume_context: dict,
) -> tuple[dict[str, float], dict]:
    title = job_context.get("title") or ""
    description = job_context.get("description") or ""
    requirements = job_context.get("requirements") or ""
    location = job_context.get("location") or ""
    remote_policy = job_context.get("remotePolicy") or ""
    tech_stack = ensure_str_list(job_context.get("techStack"))

    title_tokens = tokenize_text(title)
    job_tokens = tokenize_text(title, description, requirements, " ".join(tech_stack), location)

    target_roles = ensure_str_list(profile_context.get("targetRoles"))
    must_have_skills = ensure_str_list(profile_context.get("mustHaveSkills"))
    nice_to_have_skills = ensure_str_list(profile_context.get("niceToHaveSkills"))
    resume_skills = ensure_str_list(resume_context.get("skills"))
    preferred_locations = ensure_str_list(profile_context.get("preferredLocations"))
    preferred_work_model = profile_context.get("preferredWorkModel") or ""

    role_match = 0.0
    if target_roles:
        for role in target_roles:
            role_tokens = tokenize_text(role)
            if not role_tokens:
                continue

            overlap = len(title_tokens & role_tokens) / len(role_tokens)
            role_match = max(role_match, overlap)
    else:
        role_match = (
            1.0 if any(token in title_tokens for token in {"engineer", "developer", "backend"}) else 0.55
        )

    candidate_seniority = infer_seniority_level(
        profile_context.get("seniority"),
        resume_context.get("seniority"),
        profile_context.get("objective"),
    )
    job_seniority = infer_seniority_level(
        job_context.get("seniorityHint"),
        title,
        description,
        requirements,
    )

    title_above_marker = bool(title_tokens & ABOVE_LEVEL_TITLE_TOKENS)
    title_hard_above_marker = bool(title_tokens & HARD_ABOVE_LEVEL_TITLE_TOKENS)

    if (
        candidate_seniority
        and candidate_seniority <= 2
        and title_above_marker
    ):
        role_match *= 0.40

    candidate_skills = (must_have_skills or resume_skills)[:20]
    skill_hits = 0
    for skill in candidate_skills:
        skill_tokens = tokenize_text(skill)
        if not skill_tokens:
            continue

        if skill_tokens & job_tokens:
            skill_hits += 1

    skill_overlap = (
        (skill_hits / len(candidate_skills)) if candidate_skills else 0.55
    )

    nice_total = len(nice_to_have_skills[:20]) if nice_to_have_skills else 0
    nice_hits = 0
    for skill in nice_to_have_skills[:20]:
        skill_tokens = tokenize_text(skill)
        if skill_tokens & job_tokens:
            nice_hits += 1

    nice_overlap = (nice_hits / nice_total) if nice_total else 0.5

    role_signal = max(0.30, min(1.0, 0.50 * role_match + 0.40 * skill_overlap + 0.10 * nice_overlap))

    seniority_gap = None
    if candidate_seniority and job_seniority:
        seniority_gap = abs(job_seniority - candidate_seniority)
        if seniority_gap == 0:
            seniority_signal = 1.0
        elif seniority_gap == 1:
            seniority_signal = 0.70 if job_seniority > candidate_seniority else 0.55
        else:
            seniority_signal = 0.20
    elif candidate_seniority or job_seniority:
        seniority_signal = 0.55
    else:
        seniority_signal = 0.65

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

    loc = location_signals(location, remote_policy, preferred_locations, preferred_work_model)
    work_model_signal = loc["workModelSignal"]
    location_signal = loc["locationSignal"]
    location_match = loc["locationMatch"]
    is_remote = loc["isRemote"]
    accepts_remote = loc["acceptsRemote"]

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
        "seniorityGap": seniority_gap,
        "workModelSignal": round(max(0.0, min(1.0, work_model_signal)), 2),
        "locationSignal": round(max(0.0, min(1.0, location_signal)), 2),
        "locationMatch": bool(location_match),
        "isRemote": bool(is_remote),
        "acceptsRemote": bool(accepts_remote),
        "titleAboveMarker": bool(title_above_marker),
        "titleHardAboveMarker": bool(title_hard_above_marker),
    }

    return signals, details


def reason_from_signals(signals: dict[str, float], details: dict) -> str:
    return (
        "signals="
        f"role:{signals['role_weight']:.2f},"
        f"salary:{signals['salary_weight']:.2f},"
        f"location:{signals['location_weight']:.2f},"
        f"seniority:{signals['seniority_weight']:.2f}"
        f" | role_match:{float(details.get('roleMatch') or 0):.2f}"
        f" skills:{int(details.get('skillHits') or 0)}/{int(details.get('skillTotal') or 0)}"
        f" seniority:{details.get('candidateSeniority')}->{details.get('jobSeniority')}"
        f" location_match:{int(bool(details.get('locationMatch')))}"
        f" remote:{int(bool(details.get('isRemote')))}"
    )


def compute_score(weights: dict[str, float], signals: dict[str, float]) -> float:
    score = 0.0
    for key, weight in weights.items():
        score += max(0.0, float(weight)) * max(0.0, float(signals.get(key, 0.0))) * 100
    return scoring_policy.clamp_score(score)


def build_ai_context_hash(job_context: dict, profile_context: dict, resume_context: dict) -> str:
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
            "techStack": ensure_str_list(job_context.get("techStack")),
            "source": job_context.get("source"),
            "sourceUrl": job_context.get("sourceUrl"),
        },
        "profile": {
            "objective": profile_context.get("objective"),
            "seniority": profile_context.get("seniority"),
            "targetRoles": ensure_str_list(profile_context.get("targetRoles")),
            "preferredLocations": ensure_str_list(profile_context.get("preferredLocations")),
            "preferredWorkModel": profile_context.get("preferredWorkModel"),
            "salaryExpectation": profile_context.get("salaryExpectation"),
            "mustHaveSkills": ensure_str_list(profile_context.get("mustHaveSkills")),
            "niceToHaveSkills": ensure_str_list(profile_context.get("niceToHaveSkills")),
        },
        "resume": {
            "summary": resume_context.get("summary"),
            "seniority": resume_context.get("seniority"),
            "skills": ensure_str_list(resume_context.get("skills")),
            "languages": ensure_str_list(resume_context.get("languages")),
            "experience": resume_context.get("experience") or [],
            "education": resume_context.get("education") or [],
            "parseStatus": resume_context.get("parseStatus"),
        },
    }

    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _hash_payload(payload: dict) -> str:
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_ai_cache_versions(
    job_context: dict,
    profile_context: dict,
    resume_context: dict,
    scoring_prompt: str,
    model_name: str,
) -> dict[str, str]:
    job_payload = {
        "title": job_context.get("title"),
        "company": job_context.get("company"),
        "location": job_context.get("location"),
        "description": job_context.get("description"),
        "requirements": job_context.get("requirements"),
        "employmentType": job_context.get("employmentType"),
        "seniorityHint": job_context.get("seniorityHint"),
        "remotePolicy": job_context.get("remotePolicy"),
        "techStack": ensure_str_list(job_context.get("techStack")),
        "source": job_context.get("source"),
        "sourceUrl": job_context.get("sourceUrl"),
    }

    profile_payload = {
        "objective": profile_context.get("objective"),
        "seniority": profile_context.get("seniority"),
        "targetRoles": ensure_str_list(profile_context.get("targetRoles")),
        "preferredLocations": ensure_str_list(profile_context.get("preferredLocations")),
        "preferredWorkModel": profile_context.get("preferredWorkModel"),
        "salaryExpectation": profile_context.get("salaryExpectation"),
        "mustHaveSkills": ensure_str_list(profile_context.get("mustHaveSkills")),
        "niceToHaveSkills": ensure_str_list(profile_context.get("niceToHaveSkills")),
    }

    resume_payload = {
        "summary": resume_context.get("summary"),
        "seniority": resume_context.get("seniority"),
        "skills": ensure_str_list(resume_context.get("skills")),
        "languages": ensure_str_list(resume_context.get("languages")),
        "experience": resume_context.get("experience") or [],
        "education": resume_context.get("education") or [],
        "parseStatus": resume_context.get("parseStatus"),
        "parseConfidence": resume_context.get("parseConfidence"),
    }

    job_hash = _hash_payload(job_payload)
    profile_version = _hash_payload(profile_payload)
    resume_version = _hash_payload(resume_payload)
    prompt_version = hashlib.sha256(str(scoring_prompt or "").encode("utf-8")).hexdigest()
    model_version = str(model_name or "").strip().lower()

    cache_key_payload = {
        "jobHash": job_hash,
        "profileVersion": profile_version,
        "resumeVersion": resume_version,
        "promptVersion": prompt_version,
        "modelVersion": model_version,
    }

    return {
        "jobHash": job_hash,
        "profileVersion": profile_version,
        "resumeVersion": resume_version,
        "promptVersion": prompt_version,
        "modelVersion": model_version,
        "cacheKey": _hash_payload(cache_key_payload),
    }


def evaluate_ai_prefilter(
    deterministic_score: float,
    signal_details: dict,
    min_deterministic_score: float,
    min_role_match: float,
    min_skill_overlap: float,
    min_location_signal: float,
    min_work_model_signal: float,
    max_seniority_gap: int,
) -> dict:
    role_match = float(signal_details.get("roleMatch") or 0.0)
    skill_hits = int(signal_details.get("skillHits") or 0)
    skill_total = int(signal_details.get("skillTotal") or 0)
    skill_overlap = (skill_hits / skill_total) if skill_total > 0 else 0.0

    location_signal = float(signal_details.get("locationSignal") or 0.0)
    work_model_signal = float(signal_details.get("workModelSignal") or 0.0)

    seniority_gap_value = signal_details.get("seniorityGap")
    seniority_gap = int(seniority_gap_value) if seniority_gap_value is not None else None

    candidate_seniority = signal_details.get("candidateSeniority")
    title_hard_above = bool(signal_details.get("titleHardAboveMarker"))

    reason = None
    if float(deterministic_score) < float(min_deterministic_score):
        reason = "below_deterministic_threshold"
    elif role_match < float(min_role_match):
        reason = "low_role_match"
    elif skill_total > 0 and skill_overlap < float(min_skill_overlap):
        reason = "low_skill_overlap"
    elif work_model_signal < float(min_work_model_signal):
        reason = "low_work_model_signal"
    elif location_signal < float(min_location_signal):
        reason = "low_location_signal"
    elif seniority_gap is not None and seniority_gap > max(0, int(max_seniority_gap)):
        reason = "high_seniority_gap"
    elif (
        candidate_seniority
        and int(candidate_seniority) <= 2
        and title_hard_above
    ):
        reason = "title_above_candidate_level"

    return {
        "allowAi": reason is None,
        "reason": reason,
        "metrics": {
            "roleMatch": role_match,
            "skillOverlap": skill_overlap,
            "skillHits": skill_hits,
            "skillTotal": skill_total,
            "locationSignal": location_signal,
            "workModelSignal": work_model_signal,
            "seniorityGap": seniority_gap,
        },
    }


def context_quality(job_context: dict, profile_context: dict, resume_context: dict) -> float:
    job_quality = (
        0.15 * bool((job_context.get("title") or "").strip())
        + 0.10 * bool((job_context.get("company") or "").strip())
        + 0.10 * bool((job_context.get("location") or "").strip())
        + 0.35 * bool((job_context.get("description") or "").strip())
        + 0.20 * bool((job_context.get("requirements") or "").strip())
        + 0.10 * bool(ensure_str_list(job_context.get("techStack")))
    )

    profile_quality = (
        0.20 * bool((profile_context.get("objective") or "").strip())
        + 0.20 * bool((profile_context.get("seniority") or "").strip())
        + 0.20 * bool(ensure_str_list(profile_context.get("mustHaveSkills")))
        + 0.15 * bool(ensure_str_list(profile_context.get("targetRoles")))
        + 0.10 * bool((profile_context.get("salaryExpectation") or "").strip())
        + 0.15 * bool((profile_context.get("preferredWorkModel") or "").strip())
    )

    resume_quality = (
        0.30 * bool((resume_context.get("summary") or "").strip())
        + 0.35 * bool(ensure_str_list(resume_context.get("skills")))
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


def compose_final_score(
    deterministic_score: float,
    ai_score: float | None,
    ai_confidence: float | None,
    context_quality_score: float,
) -> tuple[float, float, bool]:
    if ai_score is None:
        return scoring_policy.clamp_score(deterministic_score), 0.0, False

    confidence = max(0.0, min(1.0, float(ai_confidence or 0.0)))
    quality = max(0.0, min(1.0, float(context_quality_score or 0.0)))

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
