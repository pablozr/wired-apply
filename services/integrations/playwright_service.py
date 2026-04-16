from core.logger.logger import logger
from urllib.parse import parse_qs, urlparse

FORM_SCHEMA_EVAL_SCRIPT = """
() => {
  const text = (value) => (value || '').toString().trim();
  const all = Array.from(document.querySelectorAll('form input, form textarea, form select'));
  const elements = all.length ? all : Array.from(document.querySelectorAll('input, textarea, select'));
  const ignored = new Set(['hidden', 'submit', 'button', 'image', 'reset']);
  const fields = [];
  const seen = new Set();

  for (let i = 0; i < elements.length; i += 1) {
    const el = elements[i];
    const tag = text(el.tagName).toLowerCase();
    const type = text(el.getAttribute('type') || tag).toLowerCase();
    if (ignored.has(type)) {
      continue;
    }

    const name = text(el.getAttribute('name'));
    const id = text(el.getAttribute('id'));
    const placeholder = text(el.getAttribute('placeholder'));
    const ariaLabel = text(el.getAttribute('aria-label'));

    let label = '';
    if (id) {
      const escapedId = id.replace(/"/g, '\\"');
      const directLabel = document.querySelector('label[for="' + escapedId + '"]');
      if (directLabel) {
        label = text(directLabel.innerText || directLabel.textContent);
      }
    }

    if (!label) {
      const wrapperLabel = el.closest('label');
      if (wrapperLabel) {
        label = text(wrapperLabel.innerText || wrapperLabel.textContent);
      }
    }

    if (!label) {
      label = ariaLabel;
    }

    const required = Boolean(
      el.required ||
      text(el.getAttribute('required')).toLowerCase() === 'required' ||
      text(el.getAttribute('aria-required')).toLowerCase() === 'true'
    );

    const options =
      tag === 'select'
        ? Array.from(el.options || [])
            .map((option) => text(option.textContent || option.value))
            .filter(Boolean)
            .slice(0, 25)
        : [];

    const key = [name, id, label, type].join('|').toLowerCase();
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    fields.push({
      index: i + 1,
      tag,
      type,
      name,
      id,
      label,
      placeholder,
      required,
      options,
    });
  }

  return {
    pageTitle: text(document.title),
    fields,
  };
}
"""


def detect_ats_provider(source_url: str) -> str:
    normalized = (source_url or "").strip().lower()
    if not normalized:
        return "UNKNOWN"

    parsed = urlparse(normalized)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    query = parse_qs(parsed.query or "")

    if "gh_jid" in query:
        return "GREENHOUSE"

    if "greenhouse.io" in host or "greenhouse.io" in normalized:
        return "GREENHOUSE"

    if "job" in path and "gh_" in (parsed.query or ""):
        return "GREENHOUSE"

    if "lever.co" in host or "lever.co" in normalized:
        return "LEVER"

    if "ashbyhq.com" in host or "ashbyhq.com" in normalized:
        return "ASHBY"

    return "UNKNOWN"


async def extract_apply_form_schema(source_url: str, timeout_ms: int = 20000) -> dict:
    provider = detect_ats_provider(source_url)

    if not source_url:
        return {
            "status": False,
            "message": "Apply URL is required",
            "data": {"provider": provider, "fields": []},
        }

    try:
        from playwright.async_api import async_playwright
    except Exception as error:
        return {
            "status": False,
            "message": f"Playwright is unavailable: {error}",
            "data": {
                "provider": provider,
                "fields": [],
                "errorCode": "PLAYWRIGHT_IMPORT_FAILED",
            },
        }

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.goto(source_url, wait_until="domcontentloaded", timeout=timeout_ms)
                await page.wait_for_timeout(1200)

                extraction = await page.evaluate(FORM_SCHEMA_EVAL_SCRIPT)
                fields = extraction.get("fields") or []

                if not fields:
                    for selector in [
                        "button:has-text('Apply')",
                        "button:has-text('Apply Now')",
                        "a:has-text('Apply')",
                        "a:has-text('Apply Now')",
                        "a[href*='apply']",
                    ]:
                        locator = page.locator(selector)
                        if await locator.count() <= 0:
                            continue

                        try:
                            await locator.first.click(timeout=2500)
                            await page.wait_for_timeout(1200)
                        except Exception:
                            continue

                        extraction = await page.evaluate(FORM_SCHEMA_EVAL_SCRIPT)
                        fields = extraction.get("fields") or []
                        if fields:
                            break

                required_fields = [field for field in fields if field.get("required")]
                return {
                    "status": True,
                    "message": "Apply form schema extracted",
                    "data": {
                        "provider": provider,
                        "sourceUrl": source_url,
                        "pageTitle": extraction.get("pageTitle") or "",
                        "fields": fields,
                        "totalFields": len(fields),
                        "requiredFields": len(required_fields),
                        "optionalFields": max(0, len(fields) - len(required_fields)),
                    },
                }
            finally:
                await browser.close()
    except Exception as error:
        error_message = str(error)
        error_code = None
        if "Executable doesn't exist" in error_message:
            error_code = "PLAYWRIGHT_BROWSER_MISSING"

        logger.warning("playwright_extract_schema_failed url=%s error=%s", source_url, error)
        return {
            "status": False,
            "message": f"Failed to extract apply form schema: {error_message}",
            "data": {
                "provider": provider,
                "fields": [],
                "errorCode": error_code,
            },
        }


async def prepare_assisted_apply(
    run_id: str,
    user_id: int,
    job_id: int,
    job_context: dict,
    auto_apply_payload: dict,
) -> dict:
    source_url = job_context.get("sourceUrl")
    answers = auto_apply_payload.get("answers")
    provider = detect_ats_provider(source_url or "")

    if not source_url:
        return {
            "status": False,
            "message": "Job source URL is missing for assisted apply",
            "data": {},
        }

    answers_count = len(answers) if isinstance(answers, dict) else 0

    logger.info(
        "playwright_assisted_plan_ready run_id=%s user_id=%s job_id=%s answers=%s",
        run_id,
        user_id,
        job_id,
        answers_count,
    )

    return {
        "status": True,
        "message": "Playwright assisted apply plan prepared",
        "data": {
            "mode": "ASSISTED",
            "sourceUrl": source_url,
            "provider": provider,
            "answersCount": answers_count,
            "nextStep": "human_confirmation_required",
        },
    }


async def get_module_status() -> dict:
    return {
        "status": True,
        "message": "Playwright integration ready in assisted mode",
        "data": {
            "module": "playwright",
            "mode": "ASSISTED",
            "requiresHumanConfirmation": True,
            "supportsFormSchemaExtraction": True,
        },
    }
