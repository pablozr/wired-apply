def clamp_score(score: float) -> float:
    return max(0.0, min(100.0, score))


def bucket_from_score(score: float) -> str:
    normalized_score = clamp_score(score)

    if normalized_score >= 80:
        return "A"
    if normalized_score >= 60:
        return "B"
    return "C"
