"""
Compare the thesis telesales dataset with a controlled CallCenterEN subset.

The script generates quantitative artifacts and a report-ready Markdown section
that can be incorporated into the graduation thesis.
"""

from __future__ import annotations

import csv
import json
import re
import statistics
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
PRIMARY_DATA_GLOB = ROOT / "master_data" / "raw"
CALLCENTEREN_BASELINE_CSV = (
    ROOT
    / "92k-real-world-call-center-scripts-english"
    / "prepared_subset"
    / "baseline_analysis_sample.csv"
)
NLP_TRAIN_CSV = ROOT / "NLP model" / "train.csv"

PII_PATTERN = re.compile(r"\[([A-Z_]+)\]")


def mean(values: list[float | int]) -> float | None:
    return round(statistics.mean(values), 4) if values else None


def median(values: list[float | int]) -> float | None:
    return round(statistics.median(values), 4) if values else None


def percentile(values: list[float | int], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * pct)))
    return round(float(ordered[index]), 4)


def read_primary_records() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(PRIMARY_DATA_GLOB.glob("transcript_batch*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"WARNING: cannot parse {path}: {exc}")
            continue

        if isinstance(payload, list):
            records.extend(row for row in payload if isinstance(row, dict))
        elif isinstance(payload, dict) and isinstance(payload.get("value"), list):
            records.extend(row for row in payload["value"] if isinstance(row, dict))

    return records


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def split_call_codes(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        if value.startswith("["):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except json.JSONDecodeError:
                pass
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def summarize_primary(records: list[dict[str, Any]]) -> dict[str, Any]:
    char_counts: list[int] = []
    word_counts: list[int] = []
    durations: list[float] = []
    call_codes: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    product_names: Counter[str] = Counter()

    for row in records:
        text = str(row.get("call_transcript") or "")
        char_counts.append(len(text))
        word_counts.append(len(text.split()))

        duration = row.get("talk_time_seconds")
        if isinstance(duration, (int, float)) and not isinstance(duration, bool):
            durations.append(float(duration))

        call_codes.update(split_call_codes(row.get("call_code")))
        if row.get("call_status"):
            statuses[str(row["call_status"])] += 1
        if row.get("product_name"):
            product_names[str(row["product_name"])] += 1

    return {
        "dataset": "Thesis primary telesales dataset",
        "role": "primary dataset",
        "rows": len(records),
        "avg_chars": mean(char_counts),
        "median_chars": median(char_counts),
        "p90_chars": percentile(char_counts, 0.9),
        "avg_words": mean(word_counts),
        "median_words": median(word_counts),
        "avg_duration_seconds": mean(durations),
        "median_duration_seconds": median(durations),
        "has_task_labels": True,
        "task_label_field": "call_code",
        "top_call_codes": call_codes.most_common(30),
        "call_status_distribution": dict(statuses.most_common()),
        "product_distribution": dict(product_names.most_common(20)),
    }


def summarize_callcenteren(rows: list[dict[str, str]]) -> dict[str, Any]:
    char_counts: list[int] = []
    word_counts: list[int] = []
    durations: list[float] = []
    confidences: list[float] = []
    pii_counts: list[int] = []
    source_zip: Counter[str] = Counter()
    domains: Counter[str] = Counter()
    directions: Counter[str] = Counter()
    pii_types: Counter[str] = Counter()

    for row in rows:
        char_counts.append(int(float(row.get("char_count") or 0)))
        word_counts.append(int(float(row.get("word_count") or 0)))
        durations.append(float(row.get("audio_duration") or 0))
        confidences.append(float(row.get("confidence") or 0))
        pii_counts.append(int(float(row.get("pii_token_count") or 0)))
        source_zip[row.get("source_zip", "unknown")] += 1
        domains[row.get("source_domain", "unknown")] += 1
        directions[row.get("call_direction", "unknown")] += 1

        pii_value = row.get("pii_types") or ""
        for pii_type in [item for item in pii_value.split("|") if item]:
            pii_types[pii_type] += 1

    return {
        "dataset": "CallCenterEN controlled subset",
        "role": "external baseline and auxiliary corpus",
        "rows": len(rows),
        "avg_chars": mean(char_counts),
        "median_chars": median(char_counts),
        "p90_chars": percentile(char_counts, 0.9),
        "avg_words": mean(word_counts),
        "median_words": median(word_counts),
        "avg_duration_seconds": mean(durations),
        "median_duration_seconds": median(durations),
        "avg_asr_confidence": mean(confidences),
        "avg_pii_tokens": mean(pii_counts),
        "has_task_labels": False,
        "task_label_field": None,
        "source_zip_distribution": dict(source_zip.most_common()),
        "domain_distribution": dict(domains.most_common()),
        "direction_distribution": dict(directions.most_common()),
        "top_pii_types": pii_types.most_common(30),
    }


def load_training_label_distribution() -> Counter[str]:
    if not NLP_TRAIN_CSV.exists():
        return Counter()
    rows = read_csv_rows(NLP_TRAIN_CSV)
    labels: Counter[str] = Counter()
    for row in rows:
        labels.update(split_call_codes(row.get("call_code")))
    return labels


def write_comparison_table(primary: dict[str, Any], external: dict[str, Any]) -> None:
    path = OUTPUT_DIR / "dataset_comparison_table.csv"
    fieldnames = ["metric", "primary_telesales_dataset", "callcenteren_baseline_subset", "interpretation"]
    rows = [
        {
            "metric": "Role",
            "primary_telesales_dataset": primary["role"],
            "callcenteren_baseline_subset": external["role"],
            "interpretation": "The primary dataset supports the business task; CallCenterEN supports external validation and auxiliary learning.",
        },
        {
            "metric": "Rows",
            "primary_telesales_dataset": primary["rows"],
            "callcenteren_baseline_subset": external["rows"],
            "interpretation": "The thesis uses a task-specific dataset and a smaller controlled external subset.",
        },
        {
            "metric": "Average transcript characters",
            "primary_telesales_dataset": primary["avg_chars"],
            "callcenteren_baseline_subset": external["avg_chars"],
            "interpretation": "CallCenterEN calls are longer, reflecting real-world customer-service conversations.",
        },
        {
            "metric": "Average duration seconds",
            "primary_telesales_dataset": primary["avg_duration_seconds"],
            "callcenteren_baseline_subset": external["avg_duration_seconds"],
            "interpretation": "Both datasets model telephone conversations with measurable call duration.",
        },
        {
            "metric": "Task-specific labels",
            "primary_telesales_dataset": primary["task_label_field"],
            "callcenteren_baseline_subset": "not available",
            "interpretation": "CallCenterEN cannot replace the primary dataset because it lacks call_code labels.",
        },
        {
            "metric": "PII signal",
            "primary_telesales_dataset": "customer PII fields masked in Silver layer",
            "callcenteren_baseline_subset": f"avg {external['avg_pii_tokens']} redaction tokens per sample",
            "interpretation": "The external corpus supports the thesis decision to include PII masking in the data pipeline.",
        },
        {
            "metric": "Training role",
            "primary_telesales_dataset": "supervised fine-tuning and evaluation",
            "callcenteren_baseline_subset": "domain adaptation or pseudo-label auxiliary training",
            "interpretation": "The external corpus can participate in training without becoming the primary source of truth.",
        },
    ]

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_call_code_distribution(labels: Counter[str]) -> None:
    path = OUTPUT_DIR / "call_code_distribution.csv"
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["call_code", "count"])
        writer.writeheader()
        for label, count in labels.most_common():
            writer.writerow({"call_code": label, "count": count})


def format_counter_table(counter_items: list[tuple[str, int]], limit: int = 10) -> str:
    lines = ["| Item | Count |", "|---|---:|"]
    for item, count in counter_items[:limit]:
        lines.append(f"| `{item}` | {count:,} |")
    return "\n".join(lines)


def write_report_section(primary: dict[str, Any], external: dict[str, Any], labels: Counter[str]) -> None:
    path = OUTPUT_DIR / "report_dataset_baseline_section.md"

    markdown = f"""# External Baseline Dataset Analysis: CallCenterEN

## Purpose

The thesis dataset remains the primary dataset because it contains the business
entities and task-specific labels required by the telesales lakehouse scenario:
customer profiles, offers, call logs, transcripts, and `call_code` labels. The
CallCenterEN corpus is used in two supporting roles: first, as an external
real-world reference baseline for validating the structure of call-center
transcript data; second, as an auxiliary corpus for future domain-adaptive
pretraining or weakly supervised pseudo-label experiments.

This separation is important methodologically. CallCenterEN provides real-world
call-center language, but it does not contain the same telesales business
entities or the same `call_code` taxonomy. Therefore, it should not replace the
primary dataset and should not be mixed into the primary evaluation test set.

## Academic Basis

CallCenterEN is a public real-world English call-center transcript corpus
introduced by Dao et al. (2025). The paper reports 91,706 conversations and
10,448 audio hours, intended for customer support and sales AI research. The
dataset includes inbound and outbound calls, ASR confidence, word-level
timestamps, and PII-redacted transcripts. This makes it a suitable external
baseline for validating whether transcript-centered data engineering decisions
in this thesis are consistent with real call-center data.

The proposed use as an auxiliary corpus follows two established research
directions. Domain-adaptive pretraining is supported by Gururangan et al. (2020),
who show that continued pretraining on domain data can improve downstream task
performance. Weak labeling and pseudo-labeling are supported by data programming
and self-training literature: Ratner et al. (2016) formalize labeling functions
as noisy weak supervision sources, while Amini et al. (2022) summarize
self-training methods that add high-confidence pseudo-labeled examples to the
training set.

## Quantitative Comparison

| Metric | Thesis Primary Dataset | CallCenterEN Baseline Subset |
|---|---:|---:|
| Role | Primary task dataset | External baseline / auxiliary corpus |
| Records | {primary['rows']:,} | {external['rows']:,} |
| Avg transcript length, chars | {primary['avg_chars']:,} | {external['avg_chars']:,} |
| Median transcript length, chars | {primary['median_chars']:,} | {external['median_chars']:,} |
| Avg word count | {primary['avg_words']:,} | {external['avg_words']:,} |
| Avg call duration, seconds | {primary['avg_duration_seconds']:,} | {external['avg_duration_seconds']:,} |
| Task-specific labels | `call_code` | Not available |
| ASR confidence | Not available | {external['avg_asr_confidence']:,} |
| Avg PII redaction tokens | Not directly encoded as tokens | {external['avg_pii_tokens']:,} |

## Interpretation

The comparison shows that the primary thesis dataset and CallCenterEN share the
core structure of call-center analytics data: conversation transcripts and call
duration. CallCenterEN is longer on average and includes ASR-specific metadata,
which reflects its origin as a real-world ASR transcript corpus. The primary
dataset is more suitable for the thesis task because it contains structured
telesales entities and explicit `call_code` labels.

This supports the dataset design of the thesis: `call_transcript` is justified
as the central unstructured text field, `talk_time_seconds` mirrors duration
metadata found in real call-center datasets, and PII masking in the Silver layer
is consistent with privacy requirements observed in CallCenterEN.

## CallCenterEN Subset Composition

### Domain Distribution

{format_counter_table(list(external['domain_distribution'].items()))}

### Direction Distribution

{format_counter_table(list(external['direction_distribution'].items()))}

### Top PII Types

{format_counter_table(external['top_pii_types'])}

## Primary Label Space

The thesis classifier is trained and evaluated using the primary dataset's
`call_code` label space. The most frequent labels are:

{format_counter_table(labels.most_common(15), 15)}

## Proposed Model Usage

The recommended experiment design is:

| Model | Training Data | Purpose |
|---|---|---|
| M0 | Primary dataset only | Supervised baseline |
| M1 | Primary dataset with improved preprocessing | Stronger local baseline |
| M2 | CallCenterEN domain-adaptive pretraining, then primary dataset fine-tuning | Test whether real call-center language improves representations |
| M3 | Primary dataset plus high-confidence pseudo-labeled CallCenterEN subset | Test weakly supervised auxiliary training |

The primary test set should remain drawn only from the thesis dataset. This
prevents weak labels from contaminating evaluation and preserves the claim that
the thesis dataset is the source of task-specific ground truth.

## References

- Dao, H., Chawla, G., Banda, R., & DeLeeuw, C. (2025). *Real-World En Call Center Transcripts Dataset with PII Redaction*. arXiv:2507.02958. https://arxiv.org/abs/2507.02958
- AIxBlock. *92k Real-World Call Center Scripts English*. Hugging Face Dataset. https://huggingface.co/datasets/AIxBlock/92k-real-world-call-center-scripts-english
- Gururangan, S., Marasović, A., Swayamdipta, S., Lo, K., Beltagy, I., Downey, D., & Smith, N. A. (2020). *Don't Stop Pretraining: Adapt Language Models to Domains and Tasks*. ACL 2020. https://aclanthology.org/2020.acl-main.740/
- Ratner, A., De Sa, C., Wu, S., Selsam, D., & Ré, C. (2016). *Data Programming: Creating Large Training Sets, Quickly*. NeurIPS 2016. https://papers.neurips.cc/paper/6523-data-programming-creating-large-training-sets-quickly
- Amini, M.-R., Feofanov, V., Pauletto, L., Lies Hadjadj, E., Devijver, E., & Maximov, Y. (2022). *Self-Training: A Survey*. arXiv:2202.12040. https://arxiv.org/abs/2202.12040
"""

    path.write_text(markdown, encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not CALLCENTEREN_BASELINE_CSV.exists():
        raise FileNotFoundError(f"Missing CallCenterEN subset: {CALLCENTEREN_BASELINE_CSV}")

    primary_records = read_primary_records()
    external_rows = read_csv_rows(CALLCENTEREN_BASELINE_CSV)
    labels = load_training_label_distribution()

    primary_summary = summarize_primary(primary_records)
    external_summary = summarize_callcenteren(external_rows)

    summary = {
        "primary_dataset": primary_summary,
        "callcenteren_baseline_subset": external_summary,
        "training_label_distribution": labels.most_common(),
        "sources": [
            {
                "name": "CallCenterEN paper",
                "url": "https://arxiv.org/abs/2507.02958",
            },
            {
                "name": "CallCenterEN dataset card",
                "url": "https://huggingface.co/datasets/AIxBlock/92k-real-world-call-center-scripts-english",
            },
            {
                "name": "Don't Stop Pretraining",
                "url": "https://aclanthology.org/2020.acl-main.740/",
            },
            {
                "name": "Data Programming",
                "url": "https://papers.neurips.cc/paper/6523-data-programming-creating-large-training-sets-quickly",
            },
            {
                "name": "Self-Training survey",
                "url": "https://arxiv.org/abs/2202.12040",
            },
        ],
    }

    (OUTPUT_DIR / "dataset_comparison_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_comparison_table(primary_summary, external_summary)
    write_call_code_distribution(labels)
    write_report_section(primary_summary, external_summary, labels)

    print(f"Primary records: {primary_summary['rows']:,}")
    print(f"CallCenterEN subset rows: {external_summary['rows']:,}")
    print(f"Wrote outputs to: {OUTPUT_DIR.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
