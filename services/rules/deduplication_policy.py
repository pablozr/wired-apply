def dedupe_key(
    source: str, external_job_id: str | None, title: str, company: str
) -> str:
    normalized_source = source.strip().lower()

    if external_job_id:
        normalized_external_id = external_job_id.strip().lower()
        return f"{normalized_source}:{normalized_external_id}"

    normalized_title = title.strip().lower()
    normalized_company = company.strip().lower()

    return f"{normalized_source}:{normalized_title}:{normalized_company}"
