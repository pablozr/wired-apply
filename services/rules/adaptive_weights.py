MAX_WEIGHT_VARIATION_RATIO = 0.15


def normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    if not weights:
        return {}

    sanitized = {key: max(0.0, value) for key, value in weights.items()}
    total = sum(sanitized.values())

    if total <= 0:
        equal_weight = round(1 / len(sanitized), 4)
        return {key: equal_weight for key in sanitized}

    return {key: round(value / total, 4) for key, value in sanitized.items()}


def build_delta_from_impact(
    current_weights: dict[str, float],
    impact: str,
    step: float,
) -> dict[str, float]:
    if not current_weights or step <= 0:
        return {key: 0.0 for key in current_weights}

    sorted_keys = sorted(
        current_weights,
        key=lambda key: current_weights[key],
        reverse=True,
    )
    primary_key = sorted_keys[0]
    remaining_keys = sorted_keys[1:]

    delta = {key: 0.0 for key in current_weights}

    if not remaining_keys:
        return delta

    spread = step / len(remaining_keys)

    if impact == "POSITIVE":
        delta[primary_key] = step
        for key in remaining_keys:
            delta[key] = -spread
        return delta

    if impact == "NEGATIVE":
        delta[primary_key] = -step
        for key in remaining_keys:
            delta[key] = spread
        return delta

    return delta


def apply_delta_with_guardrails(
    current_weights: dict[str, float],
    delta: dict[str, float],
    max_variation_ratio: float = MAX_WEIGHT_VARIATION_RATIO,
) -> dict[str, float]:
    if not current_weights:
        return {}

    adjusted_weights: dict[str, float] = {}

    for key, current_value in current_weights.items():
        base_value = max(0.0, current_value)
        proposed_value = base_value + delta.get(key, 0.0)

        if base_value == 0:
            min_allowed = 0.0
            max_allowed = max_variation_ratio
        else:
            min_allowed = max(0.0, base_value * (1 - max_variation_ratio))
            max_allowed = base_value * (1 + max_variation_ratio)

        adjusted_weights[key] = min(max(proposed_value, min_allowed), max_allowed)

    return normalize_weights(adjusted_weights)
