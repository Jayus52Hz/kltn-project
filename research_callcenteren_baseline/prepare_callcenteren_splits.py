"""
Build a train/valid/test split for the CallCenterEN branch.

The split is deterministic by text_hash so reruns keep the same row assignment
as more pseudo-labels arrive. Pseudo-labels are treated as trusted labels for
this branch, after configurable confidence and training-flag filters.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_SOURCE_CSV = OUTPUT_DIR / "callcenteren_15k_candidate.csv"
DEFAULT_PSEUDO_LABEL_CSV = OUTPUT_DIR / "pseudo_labels_gemini.csv"
NLP_TRAIN_CSV = ROOT / "NLP model" / "train.csv"


def split_labels(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip().upper() for item in value if str(item).strip()]
    return [item.strip().upper() for item in str(value).split(",") if item.strip()]


def as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def stable_ratio(value: str, seed: str) -> float:
    digest = hashlib.sha256(f"{seed}:{value}".encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(16**12)


def assign_split(text_hash: str, train_ratio: float, valid_ratio: float, seed: str) -> str:
    ratio = stable_ratio(text_hash, seed)
    if ratio < train_ratio:
        return "train"
    if ratio < train_ratio + valid_ratio:
        return "valid"
    return "test"


def read_primary_taxonomy() -> set[str]:
    labels: set[str] = set()
    if not NLP_TRAIN_CSV.exists():
        return labels
    for row in pd.read_csv(NLP_TRAIN_CSV).to_dict("records"):
        labels.update(split_labels(row.get("call_code")))
    return labels


def write_label_distribution(path: Path, df: pd.DataFrame) -> None:
    label_counts: Counter[str] = Counter()
    split_label_counts: Counter[tuple[str, str]] = Counter()

    for row in df[["split", "call_code"]].to_dict("records"):
        labels = split_labels(row["call_code"])
        label_counts.update(labels)
        for label in labels:
            split_label_counts[(row["split"], label)] += 1

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["split", "call_code", "count"])
        writer.writeheader()
        for label, count in label_counts.most_common():
            writer.writerow({"split": "all", "call_code": label, "count": count})
        for (split, label), count in sorted(split_label_counts.items()):
            writer.writerow({"split": split, "call_code": label, "count": count})


def display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(ROOT))
    except ValueError:
        return str(resolved)


def summarize(df: pd.DataFrame, args: argparse.Namespace) -> dict[str, Any]:
    label_counts: Counter[str] = Counter()
    for value in df["call_code"]:
        label_counts.update(split_labels(value))

    split_summary: dict[str, Any] = {}
    for split_name, split_df in df.groupby("split"):
        split_labels_counter: Counter[str] = Counter()
        for value in split_df["call_code"]:
            split_labels_counter.update(split_labels(value))

        split_summary[split_name] = {
            "rows": int(len(split_df)),
            "avg_pseudo_label_confidence": round(float(split_df["pseudo_label_confidence"].mean()), 4),
            "domain_distribution": split_df["source_domain"].fillna("unknown").value_counts().to_dict(),
            "direction_distribution": split_df["call_direction"].fillna("unknown").value_counts().to_dict(),
            "top_labels": split_labels_counter.most_common(20),
        }

    return {
        "source_csv": str(args.source_csv),
        "pseudo_label_csv": str(args.pseudo_label_csv),
        "min_confidence": args.min_confidence,
        "require_training_flag": not args.allow_not_training_flag,
        "train_ratio": args.train_ratio,
        "valid_ratio": args.valid_ratio,
        "test_ratio": round(1.0 - args.train_ratio - args.valid_ratio, 4),
        "seed": args.seed,
        "rows": int(len(df)),
        "unique_hashes": int(df["text_hash"].nunique()),
        "avg_pseudo_label_confidence": round(float(df["pseudo_label_confidence"].mean()), 4),
        "top_labels": label_counts.most_common(30),
        "split_summary": split_summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-csv", type=Path, default=DEFAULT_SOURCE_CSV)
    parser.add_argument("--pseudo-label-csv", type=Path, default=DEFAULT_PSEUDO_LABEL_CSV)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--min-confidence", type=float, default=0.80)
    parser.add_argument("--allow-not-training-flag", action="store_true")
    parser.add_argument("--train-ratio", type=float, default=0.70)
    parser.add_argument("--valid-ratio", type=float, default=0.15)
    parser.add_argument("--seed", default="callcenteren-v1")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    if args.train_ratio <= 0 or args.valid_ratio <= 0 or args.train_ratio + args.valid_ratio >= 1:
        raise ValueError("Expected train_ratio > 0, valid_ratio > 0, and train_ratio + valid_ratio < 1.")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    source = pd.read_csv(args.source_csv)
    pseudo = pd.read_csv(args.pseudo_label_csv)
    taxonomy = read_primary_taxonomy()

    pseudo["pseudo_label_confidence"] = pd.to_numeric(
        pseudo["pseudo_label_confidence"],
        errors="coerce",
    ).fillna(0.0)
    pseudo["should_use_for_training_bool"] = pseudo["should_use_for_training"].apply(as_bool)
    pseudo["label_list"] = pseudo["pseudo_call_code"].apply(split_labels)

    filtered = pseudo[
        (pseudo["pseudo_label_confidence"] >= args.min_confidence)
        & (pseudo["label_list"].apply(len) > 0)
    ].copy()
    if not args.allow_not_training_flag:
        filtered = filtered[filtered["should_use_for_training_bool"]].copy()

    if taxonomy:
        filtered["label_list"] = filtered["label_list"].apply(
            lambda labels: [label for label in labels if label in taxonomy]
        )
        filtered = filtered[filtered["label_list"].apply(len) > 0].copy()

    filtered["call_code"] = filtered["label_list"].apply(lambda labels: ", ".join(labels))

    merged = filtered.merge(
        source[
            [
                "text_hash",
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
            ]
        ],
        on="text_hash",
        how="inner",
        suffixes=("", "_source"),
    )

    merged = merged.drop_duplicates(subset=["text_hash"]).copy()
    merged = merged.sort_values("text_hash").reset_index(drop=True)
    if args.limit is not None:
        merged = merged.head(args.limit).copy()

    if merged.empty:
        raise RuntimeError("No rows remain after filtering and merge.")

    merged["split"] = merged["text_hash"].apply(
        lambda value: assign_split(str(value), args.train_ratio, args.valid_ratio, args.seed)
    )
    merged["dataset_name"] = "callcenteren"
    merged["call_transcript"] = merged["text"].fillna("").astype(str)

    output_columns = [
        "dataset_name",
        "split",
        "external_id",
        "text_hash",
        "call_transcript",
        "call_code",
        "pseudo_label_confidence",
        "should_use_for_training",
        "rationale",
        "source_zip",
        "source_entry",
        "source_domain",
        "call_direction",
        "audio_duration",
        "confidence",
        "word_count",
        "char_count",
        "pii_token_count",
        "pii_types",
    ]
    merged = merged[output_columns]

    labeled_path = args.output_dir / "callcenteren_labeled.csv"
    train_path = args.output_dir / "callcenteren_train.csv"
    valid_path = args.output_dir / "callcenteren_valid.csv"
    test_path = args.output_dir / "callcenteren_test.csv"
    summary_path = args.output_dir / "callcenteren_split_summary.json"
    distribution_path = args.output_dir / "callcenteren_split_label_distribution.csv"

    merged.to_csv(labeled_path, index=False)
    merged[merged["split"] == "train"].to_csv(train_path, index=False)
    merged[merged["split"] == "valid"].to_csv(valid_path, index=False)
    merged[merged["split"] == "test"].to_csv(test_path, index=False)

    summary = summarize(merged, args)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_label_distribution(distribution_path, merged)

    print(f"Rows after filter: {len(merged):,}")
    print(f"Train rows: {(merged['split'] == 'train').sum():,}")
    print(f"Valid rows: {(merged['split'] == 'valid').sum():,}")
    print(f"Test rows: {(merged['split'] == 'test').sum():,}")
    print(f"Wrote {display_path(labeled_path)}")
    print(f"Wrote {display_path(summary_path)}")


if __name__ == "__main__":
    main()
