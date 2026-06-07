"""
Evaluate whether pseudo-labeled CallCenterEN samples help the BoW classifier.

Evaluation is kept on the original thesis validation/test sets only. The
CallCenterEN labels are treated as weak auxiliary labels and are never used as
test ground truth.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, f1_score, hamming_loss
from sklearn.multiclass import OneVsRestClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MultiLabelBinarizer


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
NLP_DIR = ROOT / "NLP model"
TRAIN_CSV = NLP_DIR / "train.csv"
VALID_CSV = NLP_DIR / "valid.csv"
TEST_CSV = NLP_DIR / "test.csv"
AUXILIARY_CSV = (
    ROOT
    / "92k-real-world-call-center-scripts-english"
    / "prepared_subset"
    / "auxiliary_training_candidate.csv"
)
PSEUDO_LABEL_CSV = OUTPUT_DIR / "pseudo_labels_gemini.csv"


def split_labels(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if not isinstance(value, str):
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def load_primary(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["call_transcript"] = df["call_transcript"].fillna("").astype(str)
    df["label_list"] = df["call_code"].apply(split_labels)
    return df


def load_auxiliary(min_confidence: float = 0.80) -> pd.DataFrame:
    aux = pd.read_csv(AUXILIARY_CSV)
    pseudo = pd.read_csv(PSEUDO_LABEL_CSV)
    if len(pseudo) == 0:
        return pd.DataFrame(columns=["call_transcript", "call_code", "label_list", "pseudo_label_confidence"])

    pseudo = pseudo[
        (pseudo["should_use_for_training"].astype(str).str.lower() == "true")
        & (pseudo["pseudo_label_confidence"].astype(float) >= min_confidence)
    ].copy()

    merged = pseudo.merge(
        aux[["text_hash", "text"]],
        on="text_hash",
        how="inner",
    )
    merged = merged.rename(columns={"text": "call_transcript", "pseudo_call_code": "call_code"})
    merged["call_transcript"] = merged["call_transcript"].fillna("").astype(str)
    merged["label_list"] = merged["call_code"].apply(split_labels)
    return merged[["call_transcript", "call_code", "label_list", "pseudo_label_confidence"]]


def build_model() -> Pipeline:
    return Pipeline(
        steps=[
            (
                "vectorizer",
                CountVectorizer(
                    lowercase=True,
                    ngram_range=(1, 2),
                    min_df=2,
                    max_features=50000,
                ),
            ),
            (
                "classifier",
                OneVsRestClassifier(
                    LogisticRegression(
                        max_iter=1000,
                        solver="liblinear",
                        class_weight="balanced",
                    )
                ),
            ),
        ]
    )


def evaluate_model(
    model_name: str,
    train_df: pd.DataFrame,
    eval_df: pd.DataFrame,
    mlb: MultiLabelBinarizer,
) -> dict[str, Any]:
    y_train = mlb.transform(train_df["label_list"])
    y_eval = mlb.transform(eval_df["label_list"])
    model = build_model()
    model.fit(train_df["call_transcript"], y_train)
    y_pred = model.predict(eval_df["call_transcript"])

    report = classification_report(
        y_eval,
        y_pred,
        target_names=list(mlb.classes_),
        output_dict=True,
        zero_division=0,
    )

    return {
        "model": model_name,
        "train_rows": int(len(train_df)),
        "eval_rows": int(len(eval_df)),
        "subset_accuracy": float(accuracy_score(y_eval, y_pred)),
        "micro_f1": float(f1_score(y_eval, y_pred, average="micro", zero_division=0)),
        "macro_f1": float(f1_score(y_eval, y_pred, average="macro", zero_division=0)),
        "weighted_f1": float(f1_score(y_eval, y_pred, average="weighted", zero_division=0)),
        "hamming_loss": float(hamming_loss(y_eval, y_pred)),
        "classification_report": report,
    }


def write_metrics_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "model",
        "train_rows",
        "eval_rows",
        "subset_accuracy",
        "micro_f1",
        "macro_f1",
        "weighted_f1",
        "hamming_loss",
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row[field] for field in fieldnames})


def write_report_md(path: Path, valid_results: list[dict[str, Any]], test_results: list[dict[str, Any]], aux_df: pd.DataFrame) -> None:
    label_counter: Counter[str] = Counter()
    for labels in aux_df["label_list"]:
        label_counter.update(labels)

    def table(results: list[dict[str, Any]]) -> str:
        lines = [
            "| Model | Train rows | Eval rows | Subset accuracy | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for item in results:
            lines.append(
                f"| {item['model']} | {item['train_rows']:,} | {item['eval_rows']:,} | "
                f"{item['subset_accuracy']:.4f} | {item['micro_f1']:.4f} | "
                f"{item['macro_f1']:.4f} | {item['weighted_f1']:.4f} | {item['hamming_loss']:.4f} |"
            )
        return "\n".join(lines)

    label_lines = ["| Pseudo-label | Count |", "|---|---:|"]
    for label, count in label_counter.most_common(20):
        label_lines.append(f"| `{label}` | {count:,} |")

    text = f"""# Auxiliary Pseudo-Label BoW Experiment

## Setup

The experiment compares a supervised BoW baseline trained only on the primary
thesis dataset against the same BoW classifier augmented with AI-assisted
pseudo-labeled CallCenterEN samples. The evaluation sets remain the original
validation and test splits from the primary dataset.

Pseudo-labeled auxiliary rows used: {len(aux_df):,}

## Pseudo-Label Distribution

{chr(10).join(label_lines)}

## Validation Results

{table(valid_results)}

## Test Results

{table(test_results)}

## Interpretation

If the auxiliary model improves macro-F1 or per-label F1, the result supports
the claim that CallCenterEN can contribute useful real-world call-center
language signals. If the auxiliary model does not improve the primary test set,
the result is still methodologically valid because pseudo-label noise and domain
shift are expected risks in weak supervision.
"""
    path.write_text(text, encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    train_df = load_primary(TRAIN_CSV)
    valid_df = load_primary(VALID_CSV)
    test_df = load_primary(TEST_CSV)
    aux_df = load_auxiliary()

    all_primary_labels = train_df["label_list"].tolist() + valid_df["label_list"].tolist() + test_df["label_list"].tolist()
    mlb = MultiLabelBinarizer()
    mlb.fit(all_primary_labels)

    known = set(mlb.classes_)
    aux_df = aux_df.copy()
    aux_df["label_list"] = aux_df["label_list"].apply(lambda labels: [label for label in labels if label in known])
    aux_df = aux_df[aux_df["label_list"].apply(len) > 0].copy()

    primary_train = train_df[["call_transcript", "call_code", "label_list"]].copy()
    augmented_train = pd.concat(
        [
            primary_train,
            aux_df[["call_transcript", "call_code", "label_list"]],
        ],
        ignore_index=True,
    )

    valid_results = [
        evaluate_model("M0_primary_only_bow", primary_train, valid_df, mlb),
        evaluate_model("M3_primary_plus_callcenteren_pseudo_bow", augmented_train, valid_df, mlb),
    ]
    test_results = [
        evaluate_model("M0_primary_only_bow", primary_train, test_df, mlb),
        evaluate_model("M3_primary_plus_callcenteren_pseudo_bow", augmented_train, test_df, mlb),
    ]

    summary = {
        "primary_train_rows": int(len(primary_train)),
        "auxiliary_rows_used": int(len(aux_df)),
        "augmented_train_rows": int(len(augmented_train)),
        "valid_results": valid_results,
        "test_results": test_results,
    }
    (OUTPUT_DIR / "auxiliary_bow_experiment_results.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_metrics_csv(OUTPUT_DIR / "auxiliary_bow_valid_metrics.csv", valid_results)
    write_metrics_csv(OUTPUT_DIR / "auxiliary_bow_test_metrics.csv", test_results)
    write_report_md(OUTPUT_DIR / "auxiliary_bow_experiment_report.md", valid_results, test_results, aux_df)

    print(f"Primary train rows: {len(primary_train):,}")
    print(f"Auxiliary rows used: {len(aux_df):,}")
    print("Validation:")
    for row in valid_results:
        print(f"  {row['model']}: micro-F1={row['micro_f1']:.4f}, macro-F1={row['macro_f1']:.4f}")
    print("Test:")
    for row in test_results:
        print(f"  {row['model']}: micro-F1={row['micro_f1']:.4f}, macro-F1={row['macro_f1']:.4f}")


if __name__ == "__main__":
    main()
