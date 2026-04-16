import json


def _normalize_text(value) -> str:
    return str(value or "").strip().lower()


def _list_from_value(value) -> list[str]:
    if value is None:
        return []

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []

    if not isinstance(value, list):
        return []

    normalized: list[str] = []
    seen: set[str] = set()

    for item in value:
        token = str(item).strip()
        if not token:
            continue

        dedupe_key = token.lower()
        if dedupe_key in seen:
            continue

        normalized.append(token)
        seen.add(dedupe_key)

    return normalized


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


def has_candidate_signals(candidate_context: dict) -> bool:
    target_roles = _list_from_value(candidate_context.get("targetRoles"))
    must_have_skills = _list_from_value(candidate_context.get("mustHaveSkills"))
    resume_skills = _list_from_value(candidate_context.get("resumeSkills"))

    return bool(
        target_roles
        or must_have_skills
        or resume_skills
        or _normalize_text(candidate_context.get("seniority"))
        or _normalize_text(candidate_context.get("preferredWorkModel"))
        or _list_from_value(candidate_context.get("preferredLocations"))
    )


def evaluate_job_relevance(
    raw_job: dict,
    candidate_context: dict,
    threshold: float,
    exploration_rate: float,
    random_value: float,
) -> dict:
    title = raw_job.get("title") or ""
    description = raw_job.get("description") or ""
    requirements = raw_job.get("requirements") or ""
    location = _normalize_text(raw_job.get("location"))
    remote_policy = _normalize_text(raw_job.get("remote_policy"))
    tech_stack = _list_from_value(raw_job.get("tech_stack"))

    title_tokens = _tokenize_text(title)
    job_tokens = _tokenize_text(title, description, requirements, " ".join(tech_stack), location)

    target_roles = _list_from_value(candidate_context.get("targetRoles"))
    must_have_skills = _list_from_value(candidate_context.get("mustHaveSkills"))
    nice_to_have_skills = _list_from_value(candidate_context.get("niceToHaveSkills"))
    resume_skills = _list_from_value(candidate_context.get("resumeSkills"))
    preferred_locations = _list_from_value(candidate_context.get("preferredLocations"))
    preferred_work_model = _normalize_text(candidate_context.get("preferredWorkModel"))

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

    candidate_skills = (must_have_skills or resume_skills)[:25]
    skill_hits = 0
    for skill in candidate_skills:
        skill_tokens = _tokenize_text(skill)
        if skill_tokens and skill_tokens & job_tokens:
            skill_hits += 1

    skill_overlap = (skill_hits / len(candidate_skills)) if candidate_skills else 0.55

    nice_hits = 0
    for skill in nice_to_have_skills[:20]:
        skill_tokens = _tokenize_text(skill)
        if skill_tokens and skill_tokens & job_tokens:
            nice_hits += 1

    nice_overlap = (
        (nice_hits / len(nice_to_have_skills[:20])) if nice_to_have_skills else 0.5
    )

    role_signal = max(0.25, min(1.0, 0.45 * role_match + 0.45 * skill_overlap + 0.10 * nice_overlap))

    candidate_seniority = _infer_seniority_level(
        candidate_context.get("seniority"),
        candidate_context.get("objective"),
    )
    job_seniority = _infer_seniority_level(
        raw_job.get("seniority_hint"),
        title,
        description,
        requirements,
    )

    if candidate_seniority and job_seniority:
        seniority_gap = abs(job_seniority - candidate_seniority)
        seniority_signal = 1.0 if seniority_gap == 0 else 0.72 if seniority_gap == 1 else 0.42
        if job_seniority < candidate_seniority:
            seniority_signal = max(0.35, seniority_signal - 0.08)
    elif candidate_seniority or job_seniority:
        seniority_signal = 0.65
    else:
        seniority_signal = 0.70

    is_job_remote = "remote" in location or "remote" in remote_policy
    is_job_hybrid = "hybrid" in location or "hybrid" in remote_policy
    location_signal = 0.65

    if preferred_work_model:
        if "remote" in preferred_work_model:
            location_signal = 1.0 if is_job_remote else 0.35
        elif "hybrid" in preferred_work_model:
            if is_job_hybrid:
                location_signal = 1.0
            elif is_job_remote:
                location_signal = 0.75
            else:
                location_signal = 0.50
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

    relevance_ratio = max(
        0.0,
        min(
            1.0,
            0.40 * role_signal + 0.35 * skill_overlap + 0.15 * seniority_signal + 0.10 * location_signal,
        ),
    )

    threshold_value = max(0.0, min(1.0, float(threshold)))
    exploration_value = max(0.0, min(1.0, float(exploration_rate)))

    matched_by_threshold = relevance_ratio >= threshold_value
    kept_by_exploration = (not matched_by_threshold) and (float(random_value) < exploration_value)
    keep = matched_by_threshold or kept_by_exploration

    return {
        "keep": keep,
        "score": round(relevance_ratio * 100, 2),
        "scoreRatio": round(relevance_ratio, 4),
        "matchedByThreshold": matched_by_threshold,
        "explorationKept": kept_by_exploration,
        "reason": (
            "relevance="
            f"{relevance_ratio:.3f};role={role_match:.2f};skills={skill_hits}/{len(candidate_skills)};"
            f"seniority={candidate_seniority}->{job_seniority};"
            f"exploration={'1' if kept_by_exploration else '0'}"
        ),
        "details": {
            "roleMatch": round(role_match, 2),
            "skillHits": skill_hits,
            "skillTotal": len(candidate_skills),
            "candidateSeniority": candidate_seniority,
            "jobSeniority": job_seniority,
        },
    }
