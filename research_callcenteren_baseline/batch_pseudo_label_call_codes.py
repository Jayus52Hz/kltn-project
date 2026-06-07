"""
Batch pseudo-label CallCenterEN transcripts with Google AI Studio models.

This script is optimized for models such as `gemma-4-31b-it` that may return
thought parts before the final JSON. It labels multiple transcripts per request,
resumes from an existing output CSV, and appends only new labels.

Example, 15 RPM limit:

  $env:GEMINI_API_KEY="YOUR_KEY_HERE"
  python .\\research_callcenteren_baseline\\batch_pseudo_label_call_codes.py --target-total 300 --batch-size 8 --sleep 4.2
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
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "pseudo_labels_gemini.csv"
DEFAULT_OUTPUT_JSONL = OUTPUT_DIR / "pseudo_labels_gemini.jsonl"
DEFAULT_ERROR_JSONL = OUTPUT_DIR / "pseudo_labels_gemini_errors.jsonl"

API_URL_TEMPLATE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
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
    return [label for label, _ in labels.most_common()]


def truncate_text(text: str, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(" ", 1)[0] + " ..."


def build_batch_prompt(rows: list[dict[str, str]], labels: list[str], max_chars: int) -> str:
    label_text = ", ".join(labels)
    items = []
    for idx, row in enumerate(rows):
        items.append(
            {
                "row_index": idx,
                "external_id": row.get("external_id", ""),
                "source_domain": row.get("source_domain", ""),
                "call_direction": row.get("call_direction", ""),
                "audio_duration": row.get("audio_duration", ""),
                "asr_confidence": row.get("confidence", ""),
                "transcript": truncate_text(row.get("text", ""), max_chars),
            }
        )

    return f"""You are labeling call-center transcripts for an academic telesales NLP experiment.

Assign 2 to 5 labels from the allowed `call_code` taxonomy to each transcript.
These are weak/pseudo labels, not ground truth. Be conservative.

Allowed labels:
{label_text}

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

Return only valid JSON with this schema:
{{
  "items": [
    {{
      "row_index": 0,
      "pseudo_call_codes": ["LABEL_1", "LABEL_2"],
      "confidence": 0.0,
      "rationale": "short explanation under 35 words",
      "should_use_for_training": true
    }}
  ]
}}

Input transcripts:
{json.dumps(items, ensure_ascii=False)}
"""


def extract_response_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for candidate in payload.get("candidates") or []:
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
        return json.loads(match.group(0))


def call_gemini_batch(prompt: str, model: str, api_key: str, timeout: int, retries: int) -> dict[str, Any]:
    url = API_URL_TEMPLATE.format(model=model)
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "topP": 0.9,
            "maxOutputTokens": 8192,
            "responseMimeType": "application/json",
        },
    }
    data = json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json", "x-goog-api-key": api_key}

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            return parse_json_response(extract_response_text(payload))
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Gemini batch request failed after {retries + 1} attempts: {last_error}")


def normalize_item(item: dict[str, Any], source_row: dict[str, str], allowed_labels: set[str]) -> dict[str, Any]:
    raw_codes = item.get("pseudo_call_codes") or []
    if isinstance(raw_codes, str):
        raw_codes = split_call_codes(raw_codes)

    codes: list[str] = []
    for code in raw_codes:
        code = str(code).strip().upper()
        if code in allowed_labels and code not in codes:
            codes.append(code)

    try:
        confidence = float(item.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    should_use = bool(item.get("should_use_for_training", False))
    if len(codes) < 2 or confidence < 0.65:
        should_use = False

    return {
        "external_id": source_row.get("external_id", ""),
        "source_zip": source_row.get("source_zip", ""),
        "source_domain": source_row.get("source_domain", ""),
        "call_direction": source_row.get("call_direction", ""),
        "text_hash": source_row.get("text_hash", ""),
        "pseudo_call_code": ", ".join(codes),
        "pseudo_label_confidence": round(confidence, 4),
        "should_use_for_training": should_use,
        "rationale": str(item.get("rationale") or "")[:300],
    }


def append_csv(path: Path, rows: list[dict[str, Any]]) -> None:
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
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=AUXILIARY_CSV)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--output-jsonl", type=Path, default=DEFAULT_OUTPUT_JSONL)
    parser.add_argument("--error-jsonl", type=Path, default=DEFAULT_ERROR_JSONL)
    parser.add_argument("--model", default="gemma-4-31b-it")
    parser.add_argument("--api-key-env", default="GEMINI_API_KEY")
    parser.add_argument("--target-total", type=int, default=300)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-transcript-chars", type=int, default=2200)
    parser.add_argument("--sleep", type=float, default=4.2)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--retries", type=int, default=2)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    api_key = os.environ.get(args.api_key_env)
    if not api_key:
        raise RuntimeError(f"Missing {args.api_key_env} environment variable.")

    labels = load_label_taxonomy()
    allowed_labels = set(labels)
    all_rows = read_csv(args.input)
    existing_rows = read_csv(args.output_csv)
    seen_hashes = {row.get("text_hash") for row in existing_rows if row.get("text_hash")}
    existing_count = len(existing_rows)
    remaining_needed = max(0, args.target_total - existing_count)
    if remaining_needed == 0:
        print(f"Target already reached: {existing_count:,}/{args.target_total:,}")
        return

    candidates = [row for row in all_rows if row.get("text_hash") not in seen_hashes]
    candidates = candidates[:remaining_needed]
    print(f"Existing labels: {existing_count:,}")
    print(f"New labels needed: {len(candidates):,}")

    completed = 0
    for start in range(0, len(candidates), args.batch_size):
        batch = candidates[start : start + args.batch_size]
        prompt = build_batch_prompt(batch, labels, args.max_transcript_chars)
        try:
            response = call_gemini_batch(prompt, args.model, api_key, args.timeout, args.retries)
            items = response.get("items") if isinstance(response, dict) else None
            if not isinstance(items, list):
                raise ValueError("Response does not contain an items list.")

            normalized_rows: list[dict[str, Any]] = []
            for item in items:
                try:
                    row_index = int(item.get("row_index"))
                    source_row = batch[row_index]
                except (TypeError, ValueError, IndexError):
                    continue
                normalized_rows.append(normalize_item(item, source_row, allowed_labels))

            append_csv(args.output_csv, normalized_rows)
            append_jsonl(args.output_jsonl, normalized_rows)
            completed += len(normalized_rows)
            total_now = existing_count + completed
            print(f"Batch {start // args.batch_size + 1}: +{len(normalized_rows)} labels -> {total_now}/{args.target_total}")
        except Exception as exc:
            error_row = {
                "batch_start": start,
                "batch_size": len(batch),
                "external_ids": [row.get("external_id", "") for row in batch],
                "error": str(exc),
            }
            append_jsonl(args.error_jsonl, [error_row])
            print(f"Batch {start // args.batch_size + 1}: ERROR: {exc}")

        if args.sleep:
            time.sleep(args.sleep)

    print(f"Completed new labels this run: {completed:,}")
    print(f"Current total labels: {existing_count + completed:,}")


if __name__ == "__main__":
    main()
