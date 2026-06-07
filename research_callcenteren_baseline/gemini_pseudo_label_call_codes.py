"""
Create AI-assisted pseudo-labels for a CallCenterEN auxiliary subset.

The script calls the Gemini generateContent REST API or an OpenAI-compatible
chat completions API and asks the model to map each transcript to the thesis
`call_code` taxonomy. API keys are read only from environment variables and are
never stored in output files.

Run a dry-run first:

  python research_callcenteren_baseline/gemini_pseudo_label_call_codes.py --limit 50 --dry-run

Then call the API:

  $env:GEMINI_API_KEY="YOUR_KEY_HERE"
  python research_callcenteren_baseline/gemini_pseudo_label_call_codes.py --limit 50
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
AUXILIARY_CSV = (
    ROOT
    / "92k-real-world-call-center-scripts-english"
    / "prepared_subset"
    / "auxiliary_training_candidate.csv"
)
NLP_TRAIN_CSV = ROOT / "NLP model" / "train.csv"

DEFAULT_MODEL = "gemini-2.0-flash"
API_URL_TEMPLATE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
DEFAULT_OPENAI_COMPAT_BASE_URL = "https://api.naga.ac/v1"

DEFAULT_LABELS = [
    "OPENING",
    "NEEDS_ANALYSIS",
    "PRODUCT_PITCH",
    "OBJECTION_HANDLING",
    "CLOSING_NEGOTIATION",
    "SUCCESSFUL_SALE",
    "WARM_LEAD",
    "FOLLOW_UP_EMAIL_REQUESTED",
    "SOFT_REJECTION",
    "HARD_REJECTION",
    "DO_NOT_CALL_REQUEST",
    "SUDDEN_HANG_UP",
    "SUSPICIOUS_PROBING",
    "FEE_DISCUSSION",
    "COMPETITOR_COMPARISON",
    "APATHETIC_RESPONSE",
    "ANGRY_OUTBURST",
    "DEFENSIVE_POSTURE",
    "OVERWHELMED_CONFUSION",
    "ANNOYED_SIGHING",
    "THREATENING_COMPLAINT",
    "DEMANDING_MANAGER",
    "FREQUENT_INTERRUPTIONS",
    "RUSHING_THE_CALL",
    "STALLING_FOR_TIME",
    "ACTIVE_LISTENING",
    "CURIOUS_EXPLORATION",
    "PASSIVE_AGREEMENT",
    "ENTHUSIASTIC_AGREEMENT",
    "INDECISIVE_FLIPPING",
    "MISUNDERSTANDING",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def split_call_codes(value: str) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def load_label_taxonomy() -> list[str]:
    labels: Counter[str] = Counter()
    if NLP_TRAIN_CSV.exists():
        for row in read_csv(NLP_TRAIN_CSV):
            labels.update(split_call_codes(row.get("call_code", "")))

    if labels:
        return [label for label, _ in labels.most_common()]
    return DEFAULT_LABELS


def truncate_text(text: str, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(" ", 1)[0] + " ..."


def build_prompt(row: dict[str, str], labels: list[str], max_chars: int) -> str:
    labels_text = ", ".join(labels)
    transcript = truncate_text(row.get("text", ""), max_chars=max_chars)

    return f"""You are labeling call-center transcripts for an academic telesales NLP experiment.

Task:
Assign 2 to 5 labels from the allowed `call_code` taxonomy to the transcript.
The labels are weak/pseudo labels, not ground truth. Prefer conservative labels.

Allowed labels:
{labels_text}

Decision rules:
- Always include OPENING only if the transcript contains the call opening or greeting.
- Use PRODUCT_PITCH only if an offer, quote, plan, policy, loan, card, or service is actively presented.
- Use NEEDS_ANALYSIS if the agent asks about customer needs, eligibility, situation, current provider, or household/financial details.
- Use FEE_DISCUSSION if price, rate, APR, premium, payment, quote, fee, or cost is discussed.
- Use OBJECTION_HANDLING if the customer resists and the agent tries to respond.
- Use HARD_REJECTION or SOFT_REJECTION only when rejection is clear.
- Use DO_NOT_CALL_REQUEST only when the customer asks not to be contacted again.
- Use SUDDEN_HANG_UP only when the transcript indicates abrupt disconnection or hang-up.
- Use SUCCESSFUL_SALE only when there is explicit commitment, purchase, enrollment, signed-up, or application started.
- Do not invent labels that are not in the allowed list.

Return only valid minified JSON. Do not wrap it in Markdown. Do not include
line breaks inside string values. Use double quotes for every JSON string.
Use exactly this schema:
{{
  "pseudo_call_codes": ["LABEL_1", "LABEL_2"],
  "confidence": 0.0,
  "rationale": "short explanation under 40 words",
  "should_use_for_training": true
}}

Transcript metadata:
- source_domain: {row.get('source_domain', '')}
- call_direction: {row.get('call_direction', '')}
- audio_duration: {row.get('audio_duration', '')}
- asr_confidence: {row.get('confidence', '')}

Transcript:
{transcript}
"""


def extract_response_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates") or []
    parts: list[str] = []
    for candidate in candidates:
        content = candidate.get("content") or {}
        for part in content.get("parts") or []:
            if "text" in part and not part.get("thought"):
                parts.append(str(part["text"]))
    return "\n".join(parts).strip()


def parse_json_response(text: str) -> dict[str, Any]:
    clean = text.strip()
    if clean.startswith("```"):
        clean = re.sub(r"^```(?:json)?", "", clean).strip()
        clean = re.sub(r"```$", "", clean).strip()

    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", clean, re.DOTALL)
        if not match:
            raise
        candidate = match.group(0)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            # Some models occasionally emit raw line breaks in strings. Keep the
            # batch recoverable by removing ASCII control characters before a
            # final parse attempt.
            candidate = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", candidate)
            return json.loads(candidate)


def call_gemini(prompt: str, model: str, api_key: str, timeout: int, retries: int) -> dict[str, Any]:
    url = API_URL_TEMPLATE.format(model=model)
    body = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "topP": 0.9,
            "maxOutputTokens": 2048,
            "responseMimeType": "application/json",
        },
    }
    data = json.dumps(body).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key,
    }

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
            response_payload = json.loads(raw)
            response_text = extract_response_text(response_payload)
            return parse_json_response(response_text)
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(2 * (attempt + 1))

    raise RuntimeError(f"Gemini request failed after {retries + 1} attempts: {last_error}")


def call_openai_compatible(
    prompt: str,
    model: str,
    api_key: str,
    base_url: str,
    timeout: int,
    retries: int,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/chat/completions"
    body = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ],
        "temperature": 0.1,
        "top_p": 0.9,
        "max_tokens": 768,
        "response_format": {"type": "json_object"},
    }
    data = json.dumps(body).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
            payload = json.loads(raw)
            message = payload["choices"][0]["message"]["content"]
            return parse_json_response(str(message))
        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            KeyError,
            IndexError,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(2 * (attempt + 1))

    raise RuntimeError(f"OpenAI-compatible request failed after {retries + 1} attempts: {last_error}")


def normalize_result(result: dict[str, Any], row: dict[str, str], labels: set[str]) -> dict[str, Any]:
    raw_codes = result.get("pseudo_call_codes") or []
    if isinstance(raw_codes, str):
        raw_codes = split_call_codes(raw_codes)

    codes = []
    for code in raw_codes:
        code = str(code).strip().upper()
        if code in labels and code not in codes:
            codes.append(code)

    confidence = result.get("confidence", 0)
    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        confidence = 0.0

    if confidence < 0:
        confidence = 0.0
    if confidence > 1:
        confidence = 1.0

    should_use = bool(result.get("should_use_for_training", False))
    if len(codes) < 2 or confidence < 0.65:
        should_use = False

    return {
        "external_id": row.get("external_id", ""),
        "source_zip": row.get("source_zip", ""),
        "source_domain": row.get("source_domain", ""),
        "call_direction": row.get("call_direction", ""),
        "text_hash": row.get("text_hash", ""),
        "pseudo_call_code": ", ".join(codes),
        "pseudo_label_confidence": round(confidence, 4),
        "should_use_for_training": should_use,
        "rationale": str(result.get("rationale") or "")[:300],
    }


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "external_id",
        "source_zip",
        "source_domain",
        "call_direction",
        "text_hash",
        "pseudo_call_code",
        "pseudo_label_confidence",
        "should_use_for_training",
        "rationale",
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=AUXILIARY_CSV)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--provider", choices=["gemini", "openai-compatible"], default="gemini")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--base-url", default=DEFAULT_OPENAI_COMPAT_BASE_URL)
    parser.add_argument("--api-key-env", default=None)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--max-transcript-chars", type=int, default=5000)
    parser.add_argument("--sleep", type=float, default=5.0)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    rows = read_csv(args.input)
    selected = rows[args.offset : args.offset + args.limit]
    labels = load_label_taxonomy()
    label_set = set(labels)

    if args.dry_run:
        prompt_path = args.output_dir / "pseudo_label_dry_run_prompt.txt"
        prompt = build_prompt(selected[0], labels, args.max_transcript_chars) if selected else ""
        prompt_path.write_text(prompt, encoding="utf-8")
        try:
            display_path = prompt_path.resolve().relative_to(ROOT)
        except ValueError:
            display_path = prompt_path.name
        print(f"Dry run only. Wrote first prompt to: {display_path}")
        print(f"Selected rows: {len(selected)}")
        return

    api_key_env = args.api_key_env
    if api_key_env is None:
        api_key_env = "GEMINI_API_KEY" if args.provider == "gemini" else "OPENAI_COMPAT_API_KEY"

    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise RuntimeError(f"Missing {api_key_env} environment variable.")

    output_rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, Any]] = []
    for index, row in enumerate(selected, start=1):
        prompt = build_prompt(row, labels, args.max_transcript_chars)
        try:
            if args.provider == "gemini":
                result = call_gemini(prompt, args.model, api_key, args.timeout, args.retries)
            else:
                result = call_openai_compatible(
                    prompt,
                    args.model,
                    api_key,
                    args.base_url,
                    args.timeout,
                    args.retries,
                )
            normalized = normalize_result(result, row, label_set)
            output_rows.append(normalized)
            print(
                f"[{index}/{len(selected)}] {normalized['external_id']} -> "
                f"{normalized['pseudo_call_code']} ({normalized['pseudo_label_confidence']})"
            )
        except Exception as exc:
            error_rows.append(
                {
                    "external_id": row.get("external_id", ""),
                    "source_zip": row.get("source_zip", ""),
                    "error": str(exc),
                }
            )
            print(f"[{index}/{len(selected)}] {row.get('external_id', '')} -> ERROR: {exc}")
        if args.sleep:
            time.sleep(args.sleep)

    jsonl_path = args.output_dir / "pseudo_labels_gemini.jsonl"
    csv_path = args.output_dir / "pseudo_labels_gemini.csv"
    write_jsonl(jsonl_path, output_rows)
    write_csv(csv_path, output_rows)
    if error_rows:
        error_path = args.output_dir / "pseudo_labels_gemini_errors.jsonl"
        write_jsonl(error_path, error_rows)
    try:
        jsonl_display = jsonl_path.resolve().relative_to(ROOT)
        csv_display = csv_path.resolve().relative_to(ROOT)
        error_display = error_path.resolve().relative_to(ROOT) if error_rows else None
    except ValueError:
        jsonl_display = jsonl_path.name
        csv_display = csv_path.name
        error_display = error_path.name if error_rows else None
    print(f"Wrote: {jsonl_display}")
    print(f"Wrote: {csv_display}")
    if error_rows:
        print(f"Wrote errors: {error_display}")


if __name__ == "__main__":
    main()
