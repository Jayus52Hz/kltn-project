"""
Evaluate the already-trained BoW model on pseudo-labeled CallCenterEN rows.

This script does not retrain the model. It loads:
  - NLP model/models/bow_model.pkl
  - research_callcenteren_baseline/output/pseudo_labels_gemini.csv
  - research_callcenteren_baseline/output/callcenteren_15k_candidate.csv

Then it compares BoW predictions against CallCenterEN pseudo-labels and writes
summary metrics plus row-level predictions for manual inspection.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, f1_score, hamming_loss


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_MODEL_PATH = ROOT / "NLP model" / "models" / "bow_model.pkl"
DEFAULT_PSEUDO_LABEL_CSV = OUTPUT_DIR / "pseudo_labels_gemini.csv"
DEFAULT_SOURCE_CSV = OUTPUT_DIR / "callcenteren_15k_candidate.csv"
DEFAULT_PREDICTIONS_CSV = OUTPUT_DIR / "trained_bow_on_callcenteren_predictions.csv"
DEFAULT_METRICS_JSON = OUTPUT_DIR / "trained_bow_on_callcenteren_metrics.json"
DEFAULT_REPORT_MD = OUTPUT_DIR / "trained_bow_on_callcenteren_report.md"


def split_labels(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip().upper() for item in value if str(item).strip()]
    return [item.strip().upper() for item in str(value).split(",") if item.strip()]


def as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    return len(left & right) / len(union) if union else 0.0


def load_eval_rows(
    pseudo_label_csv: Path,
    source_csv: Path,
    min_confidence: float,
    require_training_flag: bool,
    limit: int | None,
) -> pd.DataFrame:
    pseudo = pd.read_csv(pseudo_label_csv)
    source = pd.read_csv(source_csv)

    pseudo["pseudo_label_confidence"] = pseudo["pseudo_label_confidence"].apply(as_float)
    pseudo["should_use_for_training_bool"] = pseudo["should_use_for_training"].apply(as_bool)
    pseudo["label_list"] = pseudo["pseudo_call_code"].apply(split_labels)

    filtered = pseudo[
        (pseudo["pseudo_label_confidence"] >= min_confidence)
        & (pseudo["label_list"].apply(len) > 0)
    ].copy()

    if require_training_flag:
        filtered = filtered[filtered["should_use_for_training_bool"]].copy()

    merged = filtered.merge(
        source[
            [
                "text_hash",
                "text",
                "source_zip",
                "source_domain",
                "call_direction",
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

    if limit is not None:
        merged = merged.head(limit).copy()

    merged["text"] = merged["text"].fillna("").astype(str)
    return merged


def write_predictions_csv(path: Path, df: pd.DataFrame) -> None:
    fieldnames = [
        "external_id",
        "source_domain",
        "call_direction",
        "pseudo_label_confidence",
        "pseudo_call_code",
        "bow_predicted_call_code",
        "exact_match",
        "jaccard",
        "text_hash",
        "transcript_preview",
        "rationale",
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in df.to_dict("records"):
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-path", type=Path, default=DEFAULT_MODEL_PATH)
    parser.add_argument("--pseudo-label-csv", type=Path, default=DEFAULT_PSEUDO_LABEL_CSV)
    parser.add_argument("--source-csv", type=Path, default=DEFAULT_SOURCE_CSV)
    parser.add_argument("--min-confidence", type=float, default=0.80)
    parser.add_argument("--allow-not-training-flag", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--predictions-csv", type=Path, default=DEFAULT_PREDICTIONS_CSV)
    parser.add_argument("--metrics-json", type=Path, default=DEFAULT_METRICS_JSON)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    bundle = joblib.load(args.model_path)
    vectorizer = bundle["vectorizer"]
    classifier = bundle["classifier"]
    mlb = bundle["mlb"]
    known_labels = set(str(label).upper() for label in mlb.classes_)

    eval_df = load_eval_rows(
        pseudo_label_csv=args.pseudo_label_csv,
        source_csv=args.source_csv,
        min_confidence=args.min_confidence,
        require_training_flag=not args.allow_not_training_flag,
        limit=args.limit,
    )

    eval_df["label_list"] = eval_df["label_list"].apply(
        lambda labels: [label for label in labels if label in known_labels]
    )
    eval_df = eval_df[eval_df["label_list"].apply(len) > 0].copy()

    if eval_df.empty:
        raise RuntimeError("No usable pseudo-labeled rows after filtering.")

    x_eval = vectorizer.transform(eval_df["text"])
    y_true = mlb.transform(eval_df["label_list"])
    y_pred = classifier.predict(x_eval)
    pred_label_tuples = mlb.inverse_transform(y_pred)
    pred_label_lists = [list(labels) for labels in pred_label_tuples]

    exact_matches: list[bool] = []
    jaccards: list[float] = []
    for expected, predicted in zip(eval_df["label_list"], pred_label_lists):
        expected_set = set(expected)
        predicted_set = set(predicted)
        exact_matches.append(expected_set == predicted_set)
        jaccards.append(jaccard(expected_set, predicted_set))

    eval_df["bow_predicted_call_code"] = [", ".join(labels) for labels in pred_label_lists]
    eval_df["exact_match"] = exact_matches
    eval_df["jaccard"] = [round(value, 4) for value in jaccards]
    eval_df["transcript_preview"] = eval_df["text"].str.replace(r"\s+", " ", regex=True).str.slice(0, 280)

    report = classification_report(
        y_true,
        y_pred,
        target_names=list(mlb.classes_),
        output_dict=True,
        zero_division=0,
    )

    metrics = {
        "model_path": str(args.model_path),
        "pseudo_label_csv": str(args.pseudo_label_csv),
        "source_csv": str(args.source_csv),
        "min_confidence": args.min_confidence,
        "require_training_flag": not args.allow_not_training_flag,
        "eval_rows": int(len(eval_df)),
        "subset_accuracy_exact_match": float(accuracy_score(y_true, y_pred)),
        "exact_match_rate": round(sum(exact_matches) / len(exact_matches), 6),
        "avg_jaccard": round(sum(jaccards) / len(jaccards), 6),
        "micro_f1": float(f1_score(y_true, y_pred, average="micro", zero_division=0)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "weighted_f1": float(f1_score(y_true, y_pred, average="weighted", zero_division=0)),
        "hamming_loss": float(hamming_loss(y_true, y_pred)),
        "classification_report": report,
    }

    args.metrics_json.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    write_predictions_csv(args.predictions_csv, eval_df)

    md = f"""# Trained BoW on CallCenterEN Pseudo-Labels

## Setup

- Model: `{args.model_path}`
- Source rows after filter: {len(eval_df):,}
- Pseudo-label confidence threshold: {args.min_confidence}
- Require `should_use_for_training`: {not args.allow_not_training_flag}

## Metrics

| Metric | Value |
|---|---:|
| Exact-match accuracy | {metrics['subset_accuracy_exact_match']:.4f} |
| Average Jaccard | {metrics['avg_jaccard']:.4f} |
| Micro-F1 | {metrics['micro_f1']:.4f} |
| Macro-F1 | {metrics['macro_f1']:.4f} |
| Weighted-F1 | {metrics['weighted_f1']:.4f} |
| Hamming loss | {metrics['hamming_loss']:.4f} |

## Interpretation Note

This evaluation treats CallCenterEN pseudo-labels as trusted labels for a quick
compatibility check. It measures how the already-trained thesis BoW model
transfers to the generated CallCenterEN branch without retraining.
"""
    args.report_md.write_text(md, encoding="utf-8")

    print(f"Eval rows: {len(eval_df):,}")
    print(f"Exact-match accuracy: {metrics['subset_accuracy_exact_match']:.4f}")
    print(f"Average Jaccard: {metrics['avg_jaccard']:.4f}")
    print(f"Micro-F1: {metrics['micro_f1']:.4f}")
    print(f"Macro-F1: {metrics['macro_f1']:.4f}")
    print(f"Wrote {args.metrics_json.relative_to(ROOT)}")
    print(f"Wrote {args.predictions_csv.relative_to(ROOT)}")
    print(f"Wrote {args.report_md.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

