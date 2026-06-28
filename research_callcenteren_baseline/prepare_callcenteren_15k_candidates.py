r"""
Prepare a 15k CallCenterEN candidate set for Gemini pseudo-labeling.

This script reads the local CallCenterEN ZIP archives, extracts transcript
JSON files, filters and deduplicates them, then writes a balanced candidate
CSV that can be passed to batch_pseudo_label_call_codes.py.

Example:

  python .\research_callcenteren_baseline\prepare_callcenteren_15k_candidates.py

Then label the candidates:

  $env:GEMINI_API_KEY="YOUR_KEY_HERE"
  python .\research_callcenteren_baseline\batch_pseudo_label_call_codes.py `
    --input .\research_callcenteren_baseline\output\callcenteren_15k_candidate.csv `
    --target-total 15000 --batch-size 5 --sleep 4.2 --max-transcript-chars 1600
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
import statistics
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATASET_DIR = ROOT / "92k-real-world-call-center-scripts-english"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "callcenteren_15k_candidate.csv"
DEFAULT_SUMMARY_JSON = OUTPUT_DIR / "callcenteren_15k_candidate_summary.json"

DEFAULT_ZIPS = [
    "insurance_outbound.zip",
    "auto_insurance_customer_service_inbound.zip",
    "(reupload)PII_redacted_auto_insurance_script.zip",
    "customer_service_general_inbound.zip",
    "automotive_and_healthcare_insurance_inbound.zip",
    "automotive_inbound.zip",
    "(re-uploaded)PII_Redacted_Transcripts_aixblock-automotive-stereo-inbound-104h.zip",
    "home_ervice_inbound&telecom _outbound.zip",
    "home_service_inbound.zip",
    "medical_equipment_outbound.zip",
    "medicare_inbound.zip",
]

PII_PATTERN = re.compile(r"\[([A-Z_]+)\]")
SPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True)
class CandidateConfig:
    target_size: int = 15000
    min_confidence: float = 0.85
    min_duration: float = 30.0
    max_duration: float = 900.0
    min_chars: int = 150
    max_chars: int = 8000
    min_words: int = 40
    seed: int = 42


def normalize_text(text: str) -> str:
    return SPACE_PATTERN.sub(" ", text).strip()


def text_hash(text: str) -> str:
    normalized = normalize_text(text).lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def infer_direction(zip_name: str) -> str:
    lower = zip_name.lower()
    if "outbound" in lower:
        return "outbound"
    if "inbound" in lower:
        return "inbound"
    return "unknown"


def infer_domain(zip_name: str) -> str:
    lower = zip_name.lower()
    if "auto_insurance" in lower or "insurance" in lower:
        return "insurance"
    if "customer_service" in lower:
        return "customer_service"
    if "automotive" in lower:
        return "automotive"
    if "home" in lower or "telecom" in lower:
        return "home_service_telecom"
    if "medical_equipment" in lower:
        return "medical_equipment"
    if "medicare" in lower:
        return "medicare"
    return "unknown"


def extract_row(zip_path: Path, entry: zipfile.ZipInfo, payload: dict[str, Any]) -> dict[str, Any] | None:
    text = normalize_text(str(payload.get("text") or ""))
    if not text:
        return None

    confidence = safe_float(payload.get("confidence"))
    duration = safe_float(payload.get("audio_duration"))
    words = payload.get("words")
    word_count = len(words) if isinstance(words, list) else len(text.split())
    pii_types = PII_PATTERN.findall(text)
    pii_counts = Counter(pii_types)

    return {
        "external_id": Path(entry.filename).stem,
        "source_zip": zip_path.name,
        "source_entry": entry.filename,
        "source_domain": infer_domain(zip_path.name),
        "call_direction": infer_direction(zip_path.name),
        "text": text,
        "audio_duration": duration,
        "confidence": confidence,
        "word_count": word_count,
        "char_count": len(text),
        "pii_token_count": sum(pii_counts.values()),
        "pii_types": "|".join(sorted(pii_counts)),
        "text_hash": text_hash(text),
    }


def iter_rows(dataset_dir: Path, zip_names: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for zip_name in zip_names:
        zip_path = dataset_dir / zip_name
        if not zip_path.exists():
            print(f"WARNING: missing ZIP skipped: {zip_name}")
            continue

        print(f"Reading {zip_path.name} ...")
        with zipfile.ZipFile(zip_path) as archive:
            entries = [
                entry
                for entry in archive.infolist()
                if entry.filename.endswith(".json")
                and "__MACOSX" not in entry.filename
                and not entry.is_dir()
            ]
            for entry in entries:
                try:
                    payload = json.loads(archive.read(entry).decode("utf-8", errors="replace"))
                except Exception as exc:
                    print(f"WARNING: cannot parse {entry.filename}: {exc}")
                    continue
                if isinstance(payload, dict):
                    row = extract_row(zip_path, entry, payload)
                    if row is not None:
                        rows.append(row)

    return rows


def passes_filter(row: dict[str, Any], config: CandidateConfig) -> bool:
    confidence = row["confidence"]
    duration = row["audio_duration"]
    char_count = row["char_count"]

    if confidence is None or duration is None:
        return False
    if confidence < config.min_confidence:
        return False
    if duration < config.min_duration or duration > config.max_duration:
        return False
    if char_count < config.min_chars or char_count > config.max_chars:
        return False
    if row["word_count"] < config.min_words:
        return False
    return True


def deduplicate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        key = row["text_hash"]
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def balanced_sample(rows: list[dict[str, Any]], size: int, seed: int) -> list[dict[str, Any]]:
    if len(rows) <= size:
        return list(rows)

    rng = random.Random(seed)
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[row["source_zip"]].append(row)

    selected: list[dict[str, Any]] = []
    leftovers: list[dict[str, Any]] = []
    target_per_source = max(1, size // max(1, len(by_source)))

    for source_rows in by_source.values():
        rng.shuffle(source_rows)
        selected.extend(source_rows[:target_per_source])
        leftovers.extend(source_rows[target_per_source:])

    if len(selected) < size:
        rng.shuffle(leftovers)
        selected.extend(leftovers[: size - len(selected)])

    rng.shuffle(selected)
    return selected[:size]


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"rows": 0}

    durations = [row["audio_duration"] for row in rows if row["audio_duration"] is not None]
    confidences = [row["confidence"] for row in rows if row["confidence"] is not None]
    char_counts = [row["char_count"] for row in rows]
    word_counts = [row["word_count"] for row in rows]

    def mean(values: list[float | int]) -> float | None:
        return round(statistics.mean(values), 4) if values else None

    def median(values: list[float | int]) -> float | None:
        return round(statistics.median(values), 4) if values else None

    return {
        "rows": len(rows),
        "by_source_zip": dict(Counter(row["source_zip"] for row in rows)),
        "by_domain": dict(Counter(row["source_domain"] for row in rows)),
        "by_direction": dict(Counter(row["call_direction"] for row in rows)),
        "avg_duration": mean(durations),
        "median_duration": median(durations),
        "avg_confidence": mean(confidences),
        "avg_chars": mean(char_counts),
        "median_chars": median(char_counts),
        "avg_words": mean(word_counts),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "external_id",
        "source_zip",
        "source_entry",
        "source_domain",
        "call_direction",
        "text",
        "audio_duration",
        "confidence",
        "word_count",
        "char_count",
        "pii_token_count",
        "pii_types",
        "text_hash",
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", type=Path, default=DATASET_DIR)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument("--target-size", type=int, default=15000)
    parser.add_argument("--min-confidence", type=float, default=0.85)
    parser.add_argument("--min-duration", type=float, default=30.0)
    parser.add_argument("--max-duration", type=float, default=900.0)
    parser.add_argument("--min-chars", type=int, default=150)
    parser.add_argument("--max-chars", type=int, default=8000)
    parser.add_argument("--min-words", type=int, default=40)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--zip", dest="zip_names", action="append", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = CandidateConfig(
        target_size=args.target_size,
        min_confidence=args.min_confidence,
        min_duration=args.min_duration,
        max_duration=args.max_duration,
        min_chars=args.min_chars,
        max_chars=args.max_chars,
        min_words=args.min_words,
        seed=args.seed,
    )

    dataset_dir = args.dataset_dir.resolve()
    output_csv = args.output_csv.resolve()
    summary_json = args.summary_json.resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    zip_names = args.zip_names or DEFAULT_ZIPS
    raw_rows = iter_rows(dataset_dir, zip_names)
    unique_rows = deduplicate(raw_rows)
    candidates = [row for row in unique_rows if passes_filter(row, config)]
    selected = balanced_sample(candidates, config.target_size, config.seed)

    write_csv(output_csv, selected)
    summary = {
        "config": {
            "dataset_dir": str(dataset_dir),
            "output_csv": str(output_csv),
            "zip_names": zip_names,
            "target_size": config.target_size,
            "min_confidence": config.min_confidence,
            "min_duration": config.min_duration,
            "max_duration": config.max_duration,
            "min_chars": config.min_chars,
            "max_chars": config.max_chars,
            "min_words": config.min_words,
            "seed": config.seed,
        },
        "raw_rows": len(raw_rows),
        "unique_rows": len(unique_rows),
        "candidate_rows": len(candidates),
        "selected_output": summarize(selected),
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print(f"Raw rows: {len(raw_rows):,}")
    print(f"Unique rows: {len(unique_rows):,}")
    print(f"Candidate rows: {len(candidates):,}")
    print(f"Selected rows: {len(selected):,}")
    print(f"Wrote: {display_path(output_csv)}")
    print(f"Wrote: {display_path(summary_json)}")
    if len(selected) < config.target_size:
        print(f"WARNING: selected rows are below target {config.target_size:,}. Loosen filters or add ZIPs.")


if __name__ == "__main__":
    main()
