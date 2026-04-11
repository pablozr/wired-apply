POSITIVE_IMPACT = "POSITIVE"
NEUTRAL_IMPACT = "NEUTRAL"
NEGATIVE_IMPACT = "NEGATIVE"


def is_valid_feedback_rating(rating: int) -> bool:
    return 1 <= rating <= 5


def feedback_impact_from_rating(rating: int) -> str:
    if rating >= 4:
        return POSITIVE_IMPACT
    if rating <= 2:
        return NEGATIVE_IMPACT
    return NEUTRAL_IMPACT


def delta_step_from_rating(rating: int) -> float:
    if rating in {1, 5}:
        return 0.05
    if rating in {2, 4}:
        return 0.03
    return 0.0
