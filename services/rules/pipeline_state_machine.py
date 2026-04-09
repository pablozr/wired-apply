ALLOWED_PIPELINE_TRANSITIONS = {
    "INGESTED": {"NORMALIZED", "PAUSED"},
    "NORMALIZED": {"SCORED", "PAUSED"},
    "SCORED": {"APPLY_READY", "PAUSED"},
    "APPLY_READY": {"APPLIED", "PAUSED"},
    "PAUSED": {"INGESTED", "NORMALIZED", "SCORED", "APPLY_READY"},
}


def can_transition(current_state: str, next_state: str) -> bool:
    state = current_state.strip().upper()
    target = next_state.strip().upper()

    return target in ALLOWED_PIPELINE_TRANSITIONS.get(state, set())
