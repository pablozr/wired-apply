import re
import unicodedata


_DIACRITIC_PATTERN = re.compile(r"[\u0300-\u036f]")


def normalize_text(value) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""

    decomposed = unicodedata.normalize("NFKD", text)
    return _DIACRITIC_PATTERN.sub("", decomposed)


def list_from_value(value) -> list[str]:
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


def tokenize_text(*values) -> set[str]:
    tokens: set[str] = set()

    for value in values:
        text = normalize_text(value)
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


_SENIORITY_PATTERNS_LEVEL_4 = (
    r"\bstaff\b",
    r"\bprincipal\b",
    r"\barchitect\b",
    r"\bdirector\b",
    r"\bvp\b",
    r"\bhead\b",
    r"\blead\b",
    r"\btech\s*lead\b",
    r"\btechnical\s*lead\b",
    r"\bengineering\s*manager\b",
    r"\bmanager\b",
    r"\bgerente\b",
    r"\bcoordenador\b",
)

_SENIORITY_PATTERNS_LEVEL_3 = (
    r"\bsenior\b",
    r"\bsr\.?\b",
    r"\bsnr\b",
    r"\biii\b",
    r"\biv\b",
    r"\blevel\s*[345]\b",
    r"\bl[345]\b",
    r"\bespecialista\b",
)

_SENIORITY_PATTERNS_LEVEL_2 = (
    r"\bmid\b",
    r"\bmid[-\s]?level\b",
    r"\bmiddle\b",
    r"\bpleno\b",
    r"\bii\b",
    r"\blevel\s*2\b",
    r"\bl2\b",
    r"\bregular\b",
)

_SENIORITY_PATTERNS_LEVEL_1 = (
    r"\bjunior\b",
    r"\bjr\.?\b",
    r"\bentry\b",
    r"\bentry[-\s]?level\b",
    r"\bintern\b",
    r"\binternship\b",
    r"\btrainee\b",
    r"\bestagi\w*\b",
    r"\baprendiz\b",
    r"\bgrad\b",
    r"\bgraduate\b",
    r"\bnew\s*grad\b",
    r"\blevel\s*1\b",
    r"\bl1\b",
)


def infer_seniority_level(*values) -> int | None:
    text = " ".join(normalize_text(value) for value in values if value)
    if not text:
        return None

    for pattern in _SENIORITY_PATTERNS_LEVEL_4:
        if re.search(pattern, text):
            return 4

    for pattern in _SENIORITY_PATTERNS_LEVEL_3:
        if re.search(pattern, text):
            return 3

    for pattern in _SENIORITY_PATTERNS_LEVEL_2:
        if re.search(pattern, text):
            return 2

    for pattern in _SENIORITY_PATTERNS_LEVEL_1:
        if re.search(pattern, text):
            return 1

    return None


ABOVE_LEVEL_TITLE_TOKENS = frozenset({
    "senior", "sr", "snr",
    "staff", "principal", "architect",
    "lead", "head", "manager", "director", "vp",
    "iii", "iv",
    "gerente", "coordenador", "especialista",
})


HARD_ABOVE_LEVEL_TITLE_TOKENS = frozenset({
    "staff", "principal", "head", "director", "vp", "architect",
})


def title_has_above_level_marker(title_tokens: set[str]) -> bool:
    return bool(title_tokens & ABOVE_LEVEL_TITLE_TOKENS)


def title_has_hard_above_level_marker(title_tokens: set[str]) -> bool:
    return bool(title_tokens & HARD_ABOVE_LEVEL_TITLE_TOKENS)


_BR_SYNONYMS = frozenset({
    "br", "bra", "brasil", "brazil", "brazilian", "brasileiro", "brasileira",
})

_BR_STATES_TO_NAMES = {
    "ac": "acre", "al": "alagoas", "ap": "amapa", "am": "amazonas",
    "ba": "bahia", "ce": "ceara", "df": "distrito federal", "es": "espirito santo",
    "go": "goias", "ma": "maranhao", "mt": "mato grosso", "ms": "mato grosso do sul",
    "mg": "minas gerais", "pa": "para", "pb": "paraiba", "pr": "parana",
    "pe": "pernambuco", "pi": "piaui", "rj": "rio de janeiro",
    "rn": "rio grande do norte", "rs": "rio grande do sul", "ro": "rondonia",
    "rr": "roraima", "sc": "santa catarina", "sp": "sao paulo",
    "se": "sergipe", "to": "tocantins",
}

_LATAM_SYNONYMS = frozenset({
    "latam", "latin", "america", "americas", "south", "central",
})

_REMOTE_SYNONYMS = frozenset({
    "remote", "remoto", "anywhere", "worldwide", "global", "wfh",
})

_SOFTWARE_ROLE_SYNONYMS = frozenset({
    "developer", "desenvolvedor", "engineer", "engenheiro", "software",
})

_BACKEND_ROLE_SYNONYMS = frozenset({
    "backend", "api", "server", "serverside",
})

_PLATFORM_ROLE_SYNONYMS = frozenset({
    "platform", "plataforma", "infra", "infrastructure", "devops", "sre",
})


def _expand_location_tokens(text: str) -> set[str]:
    base = normalize_text(text)
    if not base:
        return set()

    tokens = set(tokenize_text(base))
    expanded = set(tokens)

    for token in list(tokens):
        if token in _BR_STATES_TO_NAMES:
            expanded.update(tokenize_text(_BR_STATES_TO_NAMES[token]))
            expanded.update(_BR_SYNONYMS)
        if token in _BR_SYNONYMS:
            expanded.update(_BR_SYNONYMS)

    return expanded


def _is_remote_text(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False

    return any(token in normalized for token in _REMOTE_SYNONYMS)


def _is_hybrid_text(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False

    return "hybrid" in normalized or "hibrido" in normalized


def location_accepts_remote(preferred_locations: list[str]) -> bool:
    for preferred in preferred_locations or []:
        if _is_remote_text(preferred):
            return True

    return False


def work_model_prefers_remote(preferred_work_model: str) -> bool:
    return _is_remote_text(preferred_work_model)


def expand_role_tokens(tokens: set[str]) -> set[str]:
    expanded = set(tokens or set())

    if {"back", "end"}.issubset(expanded):
        expanded.update({"backend", "serverside"})

    if {"front", "end"}.issubset(expanded):
        expanded.add("frontend")

    if {"full", "stack"}.issubset(expanded):
        expanded.add("fullstack")

    if expanded & _SOFTWARE_ROLE_SYNONYMS:
        expanded.add("software_role")

    if expanded & _BACKEND_ROLE_SYNONYMS:
        expanded.add("backend_role")

    if expanded & _PLATFORM_ROLE_SYNONYMS:
        expanded.add("platform_role")

    return expanded


def location_matches(job_location: str, preferred_locations: list[str]) -> bool:
    if not preferred_locations:
        return False

    job_normalized = normalize_text(job_location)
    if not job_normalized:
        return False

    job_tokens = _expand_location_tokens(job_location)
    if not job_tokens:
        return False

    for preferred in preferred_locations:
        preferred_normalized = normalize_text(preferred)
        if not preferred_normalized:
            continue

        if preferred_normalized in job_normalized:
            return True

        preferred_tokens = _expand_location_tokens(preferred)
        meaningful = preferred_tokens - {"and", "or", "the", "of", "city", "area", "region"}

        if meaningful and meaningful.issubset(job_tokens):
            return True

        if (
            meaningful
            and meaningful & _BR_SYNONYMS
            and job_tokens & _BR_SYNONYMS
        ):
            return True

    return False


def location_signals(
    job_location: str,
    job_remote_policy: str,
    preferred_locations: list[str],
    preferred_work_model: str,
) -> dict:
    is_remote = _is_remote_text(job_location) or _is_remote_text(job_remote_policy)
    is_hybrid = _is_hybrid_text(job_location) or _is_hybrid_text(job_remote_policy)

    work_model = normalize_text(preferred_work_model)
    work_model_signal = 0.65

    if work_model:
        if "remote" in work_model or "remoto" in work_model:
            work_model_signal = 1.0 if is_remote else 0.25
        elif "hybrid" in work_model or "hibrido" in work_model:
            if is_hybrid:
                work_model_signal = 1.0
            elif is_remote:
                work_model_signal = 0.78
            else:
                work_model_signal = 0.50
        elif "onsite" in work_model or "on-site" in work_model or "presencial" in work_model:
            work_model_signal = 0.95 if not (is_remote or is_hybrid) else 0.40

    prefers_remote = work_model_prefers_remote(preferred_work_model)
    accepts_remote = location_accepts_remote(preferred_locations) or prefers_remote
    has_match = False
    location_signal = work_model_signal

    if preferred_locations:
        has_match = location_matches(job_location, preferred_locations)

        if has_match:
            location_signal = min(1.0, location_signal + 0.20)
        elif is_remote and accepts_remote:
            location_signal = max(location_signal, 0.85)
        elif is_remote and not accepts_remote:
            location_signal = min(location_signal, 0.40)
        else:
            location_signal = min(location_signal, 0.15)

    return {
        "isRemote": is_remote,
        "isHybrid": is_hybrid,
        "workModelSignal": max(0.0, min(1.0, round(work_model_signal, 4))),
        "locationSignal": max(0.0, min(1.0, round(location_signal, 4))),
        "locationMatch": has_match,
        "acceptsRemote": accepts_remote,
    }
