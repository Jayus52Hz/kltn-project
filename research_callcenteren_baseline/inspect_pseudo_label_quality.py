"""
Inspect pseudo-label quality for the CallCenterEN auxiliary corpus.

This script is safe to run while labeling is in progress. It only reads the
current pseudo-label CSV and writes a point-in-time quality snapshot.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
PSEUDO_LABEL_CSV = OUTPUT_DIR / "pseudo_labels_gemini.csv"
SOURCE_CSV = OUTPUT_DIR / "callcenteren_15k_candidate.csv"
NLP_TRAIN_CSV = ROOT / "NLP model" / "train.csv"
QUALITY_JSON = OUTPUT_DIR / "pseudo_label_quality_snapshot.json"
QUALITY_MD = OUTPUT_DIR / "pseudo_label_quality_snapshot.md"


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def split_labels(value: Any) -> list[str]:
    if not value:
        return []
    return [item.strip().upper() for item in str(value).split(",") if item.strip()]


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def load_allowed_labels() -> set[str]:
    labels: set[str] = set()
    for row in read_csv(NLP_TRAIN_CSV):
        labels.update(split_labels(row.get("call_code")))
    return labels


def bucket_confidence(score: float) -> str:
    if score >= 0.90:
        return ">=0.90"
    if score >= 0.80:
        return "0.80-0.89"
    if score >= 0.65:
        return "0.65-0.79"
    return "<0.65"


def pct(numerator: int, denominator: int) -> float:
    return round(numerator / denominator * 100, 2) if denominator else 0.0


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pseudo_rows = read_csv(PSEUDO_LABEL_CSV)
    source_rows = read_csv(SOURCE_CSV)
    allowed_labels = load_allowed_labels()
    source_by_hash = {row.get("text_hash", ""): row for row in source_rows}

    label_counts: Counter[str] = Counter()
    invalid_labels: Counter[str] = Counter()
    domain_counts: Counter[str] = Counter()
    direction_counts: Counter[str] = Counter()
    confidence_buckets: Counter[str] = Counter()

    usable_rows = 0
    duplicate_hashes = 0
    empty_label_rows = 0
    seen_hashes: set[str] = set()

    for row in pseudo_rows:
        text_hash = row.get("text_hash", "")
        if text_hash in seen_hashes:
            duplicate_hashes += 1
        seen_hashes.add(text_hash)

        labels = split_labels(row.get("pseudo_call_code"))
        if not labels:
            empty_label_rows += 1

        for label in labels:
            if label in allowed_labels:
                label_counts[label] += 1
            else:
                invalid_labels[label] += 1

        confidence = as_float(row.get("pseudo_label_confidence"))
        confidence_buckets[bucket_confidence(confidence)] += 1

        if as_bool(row.get("should_use_for_training")) and confidence >= 0.80 and labels:
            usable_rows += 1

        source = source_by_hash.get(text_hash, {})
        if source:
            domain_counts[source.get("source_domain", "unknown")] += 1
            direction_counts[source.get("call_direction", "unknown")] += 1

    total_source = len(source_rows)
    total_labeled = len(pseudo_rows)
    summary = {
        "source_rows": total_source,
        "labeled_rows": total_labeled,
        "labeling_progress_pct": pct(total_labeled, total_source),
        "unique_labeled_hashes": len(seen_hashes),
        "duplicate_hash_rows": duplicate_hashes,
        "usable_rows_confidence_ge_080": usable_rows,
        "usable_rows_pct_of_labeled": pct(usable_rows, total_labeled),
        "empty_label_rows": empty_label_rows,
        "invalid_label_rows": sum(invalid_labels.values()),
        "confidence_buckets": dict(confidence_buckets),
        "top_labels": label_counts.most_common(30),
        "invalid_labels": invalid_labels.most_common(),
        "domain_distribution": dict(domain_counts.most_common()),
        "direction_distribution": dict(direction_counts.most_common()),
    }

    QUALITY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    def table(counter_items: list[tuple[str, int]], limit: int = 15) -> str:
        lines = ["| Item | Count |", "|---|---:|"]
        for item, count in counter_items[:limit]:
            lines.append(f"| `{item}` | {count:,} |")
        return "\n".join(lines)

    md = f"""# Pseudo-Label Quality Snapshot

## Progress

| Metric | Value |
|---|---:|
| Source rows | {total_source:,} |
| Labeled rows | {total_labeled:,} |
| Labeling progress | {summary['labeling_progress_pct']:.2f}% |
| Unique labeled hashes | {len(seen_hashes):,} |
| Duplicate hash rows | {duplicate_hashes:,} |
| Usable rows, confidence >= 0.80 | {usable_rows:,} |
| Usable rows / labeled | {summary['usable_rows_pct_of_labeled']:.2f}% |
| Empty label rows | {empty_label_rows:,} |
| Invalid label values | {summary['invalid_label_rows']:,} |

## Confidence Buckets

{table(list(confidence_buckets.items()))}

## Top Pseudo Labels

{table(label_counts.most_common(30), 30)}

## Domain Distribution

{table(domain_counts.most_common())}

## Direction Distribution

{table(direction_counts.most_common())}

## Invalid Labels

{table(invalid_labels.most_common()) if invalid_labels else "No invalid labels found."}
"""
    QUALITY_MD.write_text(md, encoding="utf-8")

    print(f"Labeled rows: {total_labeled:,}/{total_source:,} ({summary['labeling_progress_pct']:.2f}%)")
    print(f"Usable rows (confidence >= 0.80): {usable_rows:,}")
    print(f"Wrote {QUALITY_JSON.relative_to(ROOT)}")
    print(f"Wrote {QUALITY_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

