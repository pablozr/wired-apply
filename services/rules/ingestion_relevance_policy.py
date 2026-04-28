from services.rules.text_normalization import (
    HARD_ABOVE_LEVEL_TITLE_TOKENS,
    ABOVE_LEVEL_TITLE_TOKENS,
    expand_role_tokens,
    infer_seniority_level,
    list_from_value,
    location_signals,
    normalize_text,
    tokenize_text,
)


def has_candidate_signals(candidate_context: dict) -> bool:
    target_roles = list_from_value(candidate_context.get("targetRoles"))
    must_have_skills = list_from_value(candidate_context.get("mustHaveSkills"))
    resume_skills = list_from_value(candidate_context.get("resumeSkills"))

    return bool(
        target_roles
        or must_have_skills
        or resume_skills
        or normalize_text(candidate_context.get("seniority"))
        or normalize_text(candidate_context.get("preferredWorkModel"))
        or list_from_value(candidate_context.get("preferredLocations"))
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
    location = raw_job.get("location") or ""
    remote_policy = raw_job.get("remote_policy") or ""
    tech_stack = list_from_value(raw_job.get("tech_stack"))

    title_tokens = tokenize_text(title)
    job_tokens = tokenize_text(title, description, requirements, " ".join(tech_stack), location)
    expanded_title_tokens = expand_role_tokens(title_tokens)
    expanded_job_tokens = expand_role_tokens(job_tokens)

    target_roles = list_from_value(candidate_context.get("targetRoles"))
    must_have_skills = list_from_value(candidate_context.get("mustHaveSkills"))
    nice_to_have_skills = list_from_value(candidate_context.get("niceToHaveSkills"))
    resume_skills = list_from_value(candidate_context.get("resumeSkills"))
    preferred_locations = list_from_value(candidate_context.get("preferredLocations"))
    preferred_work_model = candidate_context.get("preferredWorkModel") or ""

    role_match = 0.0
    if target_roles:
        for role in target_roles:
            role_tokens = expand_role_tokens(tokenize_text(role))
            if not role_tokens:
                continue

            title_overlap = len(expanded_title_tokens & role_tokens) / len(role_tokens)
            context_overlap = len(expanded_job_tokens & role_tokens) / len(role_tokens)
            overlap = max(title_overlap, context_overlap * 0.85)
            role_match = max(role_match, overlap)
    else:
        role_match = (
            1.0 if any(token in expanded_title_tokens for token in {"engineer", "developer", "backend", "software_role", "backend_role"}) else 0.55
        )

    candidate_seniority = infer_seniority_level(
        candidate_context.get("seniority"),
        candidate_context.get("objective"),
        candidate_context.get("resumeSeniority"),
    )
    job_seniority = infer_seniority_level(
        raw_job.get("seniority_hint"),
        title,
        description,
        requirements,
    )

    title_above_marker = bool(title_tokens & ABOVE_LEVEL_TITLE_TOKENS)
    if (
        candidate_seniority
        and candidate_seniority <= 2
        and title_above_marker
    ):
        role_match *= 0.40

    role_signal = max(0.20, min(1.0, role_match))

    candidate_skills = (must_have_skills or resume_skills)[:25]
    skill_hits = 0
    for skill in candidate_skills:
        skill_tokens = tokenize_text(skill)
        if skill_tokens and expand_role_tokens(skill_tokens) & expanded_job_tokens:
            skill_hits += 1

    skill_overlap = (skill_hits / len(candidate_skills)) if candidate_skills else 0.55

    nice_total = len(nice_to_have_skills[:20]) if nice_to_have_skills else 0
    nice_hits = 0
    for skill in nice_to_have_skills[:20]:
        skill_tokens = tokenize_text(skill)
        if skill_tokens and expand_role_tokens(skill_tokens) & expanded_job_tokens:
            nice_hits += 1

    nice_overlap = (nice_hits / nice_total) if nice_total else 0.50

    seniority_gap = None
    if candidate_seniority and job_seniority:
        seniority_gap = abs(job_seniority - candidate_seniority)
        if seniority_gap == 0:
            seniority_signal = 1.0
        elif seniority_gap == 1:
            seniority_signal = 0.65 if job_seniority > candidate_seniority else 0.55
        else:
            seniority_signal = 0.20
    elif candidate_seniority or job_seniority:
        seniority_signal = 0.55
    else:
        seniority_signal = 0.65

    loc = location_signals(location, remote_policy, preferred_locations, preferred_work_model)
    is_remote = loc["isRemote"]
    location_signal = loc["locationSignal"]
    work_model_signal = loc["workModelSignal"]
    location_match = loc["locationMatch"]
    accepts_remote = loc["acceptsRemote"]

    penalty_reasons: list[str] = []

    if (
        candidate_seniority
        and candidate_seniority <= 2
        and job_seniority
        and (job_seniority - candidate_seniority) >= 2
    ):
        seniority_signal = min(seniority_signal, 0.12)
        penalty_reasons.append("seniority_gap_too_high")

    if (
        candidate_seniority
        and candidate_seniority <= 2
        and not job_seniority
        and (title_tokens & HARD_ABOVE_LEVEL_TITLE_TOKENS)
    ):
        role_signal = min(role_signal, 0.15)
        penalty_reasons.append("title_above_candidate_level")

    if preferred_locations and not location_match:
        if not is_remote:
            location_signal = min(location_signal, 0.12)
            penalty_reasons.append("location_mismatch_onsite")
        elif not accepts_remote:
            location_signal = min(location_signal, 0.10)
            penalty_reasons.append("location_mismatch_remote_not_accepted")

    work_model_norm = normalize_text(preferred_work_model)
    if ("remote" in work_model_norm or "remoto" in work_model_norm) and not is_remote:
        work_model_signal = min(work_model_signal, 0.10)
        location_signal = min(location_signal, 0.10)
        penalty_reasons.append("work_model_mismatch_remote_required")

    relevance_ratio = max(
        0.0,
        min(
            1.0,
            0.30 * role_signal
            + 0.20 * skill_overlap
            + 0.05 * nice_overlap
            + 0.20 * seniority_signal
            + 0.25 * location_signal,
        ),
    )

    threshold_value = max(0.0, min(1.0, float(threshold)))
    exploration_value = max(0.0, min(1.0, float(exploration_rate)))

    matched_by_threshold = relevance_ratio >= threshold_value
    kept_by_exploration = (
        not matched_by_threshold
        and float(random_value) < exploration_value
    )
    keep = matched_by_threshold or kept_by_exploration

    reason = (
        "relevance="
        f"{relevance_ratio:.3f};role={role_match:.2f};skills={skill_hits}/{len(candidate_skills)};"
        f"seniority={candidate_seniority}->{job_seniority};"
        f"location={int(location_match)};remote={int(is_remote)};"
        f"penalties={','.join(penalty_reasons) if penalty_reasons else '0'};"
        f"exploration={'1' if kept_by_exploration else '0'}"
    )

    return {
        "keep": keep,
        "score": round(relevance_ratio * 100, 2),
        "scoreRatio": round(relevance_ratio, 4),
        "matchedByThreshold": matched_by_threshold,
        "explorationKept": kept_by_exploration,
        "hardRejected": False,
        "hardRejectReasons": [],
        "reason": reason,
        "details": {
            "roleMatch": round(role_match, 2),
            "skillHits": skill_hits,
            "skillTotal": len(candidate_skills),
            "candidateSeniority": candidate_seniority,
            "jobSeniority": job_seniority,
            "seniorityGap": seniority_gap,
            "locationMatch": location_match,
            "locationSignal": round(location_signal, 2),
            "workModelSignal": round(work_model_signal, 2),
            "isRemote": is_remote,
            "acceptsRemote": accepts_remote,
        },
    }
