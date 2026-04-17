ALLOWED_PIPELINE_TRANSITIONS = {
    "INGESTED": {"NORMALIZED", "PAUSED"},
    "NORMALIZED": {"SCORED", "PAUSED"},
    "SCORED": {"PAUSED"},
    "PAUSED": {"INGESTED", "NORMALIZED", "SCORED"},
}


def can_transition(current_state: str, next_state: str) -> bool:
    state = current_state.strip().upper()
    target = next_state.strip().upper()

    return target in ALLOWED_PIPELINE_TRANSITIONS.get(state, set())
