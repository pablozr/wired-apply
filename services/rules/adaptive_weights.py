def normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    if not weights:
        return {}

    total = sum(max(0.0, value) for value in weights.values())

    if total <= 0:
        equal_weight = round(1 / len(weights), 4)
        return {key: equal_weight for key in weights}

    return {key: round(max(0.0, value) / total, 4) for key, value in weights.items()}
