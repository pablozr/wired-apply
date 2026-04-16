from io import BytesIO


def extract_text_from_pdf_bytes(file_bytes: bytes) -> str:
    try:
        from pypdf import PdfReader
    except Exception:
        return ""

    try:
        reader = PdfReader(BytesIO(file_bytes))
    except Exception:
        return ""

    chunks: list[str] = []
    for page in reader.pages:
        text = (page.extract_text() or "").strip()
        if text:
            chunks.append(text)

    return "\n\n".join(chunks).strip()
