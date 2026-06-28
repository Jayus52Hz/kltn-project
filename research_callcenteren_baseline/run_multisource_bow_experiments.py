"""
Run BoW experiments across the primary AGI Telesales dataset and CallCenterEN.

Experiments:
  M0: train primary -> test primary
  M1: train primary -> test CallCenterEN
  M2: train CallCenterEN -> test CallCenterEN
  M3: train CallCenterEN -> test primary
  M4: train primary + CallCenterEN -> test primary and CallCenterEN

The implementation is robust for smoke tests with small --limit values: labels
that have no positive examples in a training subset are predicted as absent.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, f1_score, hamming_loss
from sklearn.preprocessing import MultiLabelBinarizer


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
NLP_DIR = ROOT / "NLP model"
DEFAULT_SPLIT_DIR = OUTPUT_DIR
DEFAULT_EXPERIMENT_DIR = OUTPUT_DIR / "multisource_bow"


def split_labels(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip().upper() for item in value if str(item).strip()]
    return [item.strip().upper() for item in str(value).split(",") if item.strip()]


def jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    return len(left & right) / len(union) if union else 0.0


def load_primary_split(path: Path, split_name: str, limit: int | None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.rename(columns={"unique_id": "record_id"})
    df["dataset_name"] = "primary_telesales"
    df["split"] = split_name
    df["call_transcript"] = df["call_transcript"].fillna("").astype(str)
    df["label_list"] = df["call_code"].apply(split_labels)
    df = df[df["label_list"].apply(len) > 0].copy()
    if limit is not None:
        df = df.head(limit).copy()
    return df[["dataset_name", "split", "record_id", "call_transcript", "call_code", "label_list"]]


def load_callcenteren_split(path: Path, split_name: str, limit: int | None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.rename(columns={"external_id": "record_id"})
    df["dataset_name"] = "callcenteren"
    df["split"] = split_name
    df["call_transcript"] = df["call_transcript"].fillna("").astype(str)
    df["label_list"] = df["call_code"].apply(split_labels)
    df = df[df["label_list"].apply(len) > 0].copy()
    if limit is not None:
        df = df.head(limit).copy()
    return df[["dataset_name", "split", "record_id", "call_transcript", "call_code", "label_list"]]


def load_taxonomy(paths: list[Path]) -> list[str]:
    labels: set[str] = set()
    for path in paths:
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if "call_code" not in df.columns:
            continue
        for value in df["call_code"]:
            labels.update(split_labels(value))
    return sorted(labels)


@dataclass
class SafeBowModel:
    labels: list[str]
    vectorizer: CountVectorizer
    classifiers: dict[str, LogisticRegression | str | None]

    def predict_matrix(self, texts: pd.Series) -> np.ndarray:
        x_eval = self.vectorizer.transform(texts.fillna("").astype(str))
        output = np.zeros((x_eval.shape[0], len(self.labels)), dtype=int)
        for idx, label in enumerate(self.labels):
            clf = self.classifiers.get(label)
            if clf == "always_one":
                output[:, idx] = 1
            elif clf is not None:
                output[:, idx] = clf.predict(x_eval)
        return output

    def predict_labels(self, texts: pd.Series) -> list[list[str]]:
        matrix = self.predict_matrix(texts)
        return [
            [label for idx, label in enumerate(self.labels) if row[idx] == 1]
            for row in matrix
        ]


def train_safe_bow(train_df: pd.DataFrame, labels: list[str], max_features: int) -> SafeBowModel:
    vectorizer = CountVectorizer(
        lowercase=True,
        ngram_range=(1, 2),
        min_df=1,
        max_features=max_features,
    )
    x_train = vectorizer.fit_transform(train_df["call_transcript"].fillna("").astype(str))

    mlb = MultiLabelBinarizer(classes=labels)
    mlb.fit([labels])
    y_train = mlb.transform(train_df["label_list"])

    classifiers: dict[str, LogisticRegression | str | None] = {}
    for idx, label in enumerate(labels):
        target = y_train[:, idx]
        positives = int(target.sum())
        if positives == 0:
            classifiers[label] = None
            continue
        if positives == len(target):
            classifiers[label] = "always_one"
            continue
        clf = LogisticRegression(
            max_iter=1000,
            solver="liblinear",
            class_weight="balanced",
        )
        clf.fit(x_train, target)
        classifiers[label] = clf

    return SafeBowModel(labels=labels, vectorizer=vectorizer, classifiers=classifiers)


def evaluate_model(
    model_name: str,
    train_name: str,
    eval_name: str,
    train_rows: int,
    model: SafeBowModel,
    eval_df: pd.DataFrame,
) -> dict[str, Any]:
    labels = model.labels
    mlb = MultiLabelBinarizer(classes=labels)
    mlb.fit([labels])

    y_true = mlb.transform(eval_df["label_list"])
    y_pred = model.predict_matrix(eval_df["call_transcript"])
    predicted_labels = model.predict_labels(eval_df["call_transcript"])

    exact_matches: list[bool] = []
    jaccards: list[float] = []
    for expected, predicted in zip(eval_df["label_list"], predicted_labels):
        expected_set = set(expected)
        predicted_set = set(predicted)
        exact_matches.append(expected_set == predicted_set)
        jaccards.append(jaccard(expected_set, predicted_set))

    report = classification_report(
        y_true,
        y_pred,
        target_names=labels,
        output_dict=True,
        zero_division=0,
    )

    return {
        "model": model_name,
        "train_dataset": train_name,
        "eval_dataset": eval_name,
        "train_rows": int(train_rows),
        "eval_rows": int(len(eval_df)),
        "subset_accuracy": float(accuracy_score(y_true, y_pred)),
        "exact_match_rate": round(sum(exact_matches) / len(exact_matches), 6) if exact_matches else 0.0,
        "avg_jaccard": round(sum(jaccards) / len(jaccards), 6) if jaccards else 0.0,
        "micro_f1": float(f1_score(y_true, y_pred, average="micro", zero_division=0)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "weighted_f1": float(f1_score(y_true, y_pred, average="weighted", zero_division=0)),
        "hamming_loss": float(hamming_loss(y_true, y_pred)),
        "classification_report": report,
    }


def write_metrics_csv(path: Path, results: list[dict[str, Any]]) -> None:
    columns = [
        "model",
        "train_dataset",
        "eval_dataset",
        "train_rows",
        "eval_rows",
        "subset_accuracy",
        "exact_match_rate",
        "avg_jaccard",
        "micro_f1",
        "macro_f1",
        "weighted_f1",
        "hamming_loss",
    ]
    pd.DataFrame([{col: item[col] for col in columns} for item in results]).to_csv(path, index=False)


def display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(ROOT))
    except ValueError:
        return str(resolved)


def write_report(path: Path, results: list[dict[str, Any]], labels: list[str]) -> None:
    lines = [
        "# Multi-Source BoW Experiment",
        "",
        f"Label taxonomy size: {len(labels)}",
        "",
        "| Model | Train dataset | Eval dataset | Train rows | Eval rows | Exact match | Avg Jaccard | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in results:
        lines.append(
            f"| {item['model']} | {item['train_dataset']} | {item['eval_dataset']} | "
            f"{item['train_rows']:,} | {item['eval_rows']:,} | "
            f"{item['exact_match_rate']:.4f} | {item['avg_jaccard']:.4f} | "
            f"{item['micro_f1']:.4f} | {item['macro_f1']:.4f} | "
            f"{item['weighted_f1']:.4f} | {item['hamming_loss']:.4f} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation Note",
            "",
            "CallCenterEN pseudo-labels are treated as trusted labels for this experiment. "
            "Primary and CallCenterEN test splits remain separate, so combined training "
            "does not leak either test set into training.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--split-dir", type=Path, default=DEFAULT_SPLIT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_EXPERIMENT_DIR)
    parser.add_argument("--limit-train", type=int, default=None)
    parser.add_argument("--limit-eval", type=int, default=None)
    parser.add_argument("--max-features", type=int, default=50000)
    parser.add_argument("--save-models", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    primary_train = load_primary_split(NLP_DIR / "train.csv", "train", args.limit_train)
    primary_valid = load_primary_split(NLP_DIR / "valid.csv", "valid", args.limit_eval)
    primary_test = load_primary_split(NLP_DIR / "test.csv", "test", args.limit_eval)
    call_train = load_callcenteren_split(args.split_dir / "callcenteren_train.csv", "train", args.limit_train)
    call_valid = load_callcenteren_split(args.split_dir / "callcenteren_valid.csv", "valid", args.limit_eval)
    call_test = load_callcenteren_split(args.split_dir / "callcenteren_test.csv", "test", args.limit_eval)

    labels = load_taxonomy(
        [
            NLP_DIR / "train.csv",
            NLP_DIR / "valid.csv",
            NLP_DIR / "test.csv",
            args.split_dir / "callcenteren_train.csv",
            args.split_dir / "callcenteren_valid.csv",
            args.split_dir / "callcenteren_test.csv",
        ]
    )
    labels_set = set(labels)
    for df in [primary_train, primary_valid, primary_test, call_train, call_valid, call_test]:
        df["label_list"] = df["label_list"].apply(lambda values: [value for value in values if value in labels_set])

    combined_train = pd.concat([primary_train, call_train], ignore_index=True)

    models = {
        "M0_primary_bow": ("primary_telesales", primary_train),
        "M2_callcenteren_bow": ("callcenteren", call_train),
        "M4_combined_bow": ("primary_telesales+callcenteren", combined_train),
    }

    trained_models: dict[str, tuple[str, SafeBowModel, int]] = {}
    for model_name, (train_name, train_df) in models.items():
        trained_models[model_name] = (
            train_name,
            train_safe_bow(train_df, labels, args.max_features),
            len(train_df),
        )

    eval_sets = {
        "primary_valid": primary_valid,
        "primary_test": primary_test,
        "callcenteren_valid": call_valid,
        "callcenteren_test": call_test,
    }

    experiments = [
        ("M0_primary_bow", "primary_valid"),
        ("M0_primary_bow", "primary_test"),
        ("M1_primary_to_callcenteren", "callcenteren_valid", "M0_primary_bow"),
        ("M1_primary_to_callcenteren", "callcenteren_test", "M0_primary_bow"),
        ("M2_callcenteren_bow", "callcenteren_valid"),
        ("M2_callcenteren_bow", "callcenteren_test"),
        ("M3_callcenteren_to_primary", "primary_valid", "M2_callcenteren_bow"),
        ("M3_callcenteren_to_primary", "primary_test", "M2_callcenteren_bow"),
        ("M4_combined_bow", "primary_valid"),
        ("M4_combined_bow", "primary_test"),
        ("M4_combined_bow", "callcenteren_valid"),
        ("M4_combined_bow", "callcenteren_test"),
    ]

    results: list[dict[str, Any]] = []
    for experiment in experiments:
        output_model_name = experiment[0]
        eval_name = experiment[1]
        trained_key = experiment[2] if len(experiment) > 2 else output_model_name
        train_name, model, train_rows = trained_models[trained_key]
        results.append(
            evaluate_model(
                model_name=output_model_name,
                train_name=train_name,
                eval_name=eval_name,
                train_rows=train_rows,
                model=model,
                eval_df=eval_sets[eval_name],
            )
        )

    metrics_json = args.output_dir / "multisource_bow_experiment_metrics.json"
    metrics_csv = args.output_dir / "multisource_bow_experiment_metrics.csv"
    report_md = args.output_dir / "multisource_bow_experiment_report.md"
    metrics_json.write_text(json.dumps({"labels": labels, "results": results}, ensure_ascii=False, indent=2), encoding="utf-8")
    write_metrics_csv(metrics_csv, results)
    write_report(report_md, results, labels)

    if args.save_models:
        model_dir = args.output_dir / "models"
        model_dir.mkdir(parents=True, exist_ok=True)
        for model_name, (_, model, _) in trained_models.items():
            joblib.dump(model, model_dir / f"{model_name}.pkl")

    print(f"Wrote {display_path(metrics_json)}")
    print(f"Wrote {display_path(metrics_csv)}")
    print(f"Wrote {display_path(report_md)}")
    for item in results:
        print(
            f"{item['model']} -> {item['eval_dataset']}: "
            f"micro-F1={item['micro_f1']:.4f}, macro-F1={item['macro_f1']:.4f}, "
            f"jaccard={item['avg_jaccard']:.4f}"
        )


if __name__ == "__main__":
    main()
