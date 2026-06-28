"""
Fine-tune a CallCenterEN-specific multilabel BoW/TF-IDF classifier.

This script trains only on the CallCenterEN branch. It tries several text
feature configurations, tunes per-label decision thresholds on the validation
split, evaluates on the held-out CallCenterEN test split, then applies the best
model to the full 15k CallCenterEN candidate set.

The output CSV is intended as the complete CallCenterEN schema artifact for the
Lakehouse branch. It keeps the original pseudo-label columns when available and
adds model-generated call_code fields for all 15k rows.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, f1_score, hamming_loss
from sklearn.preprocessing import MultiLabelBinarizer


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_SPLIT_DIR = OUTPUT_DIR
DEFAULT_CANDIDATE_CSV = OUTPUT_DIR / "callcenteren_15k_candidate.csv"
DEFAULT_PSEUDO_LABEL_CSV = OUTPUT_DIR / "pseudo_labels_gemini.csv"
DEFAULT_OUTPUT_DIR = OUTPUT_DIR / "callcenteren_finetuned"


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


class FeatureUnionLite(BaseEstimator, TransformerMixin):
    """Small serializable feature union for sparse text features."""

    def __init__(self, transformers: list[tuple[str, Any]]):
        self.transformers = transformers

    def fit(self, texts, y=None):
        for _, transformer in self.transformers:
            transformer.fit(texts)
        return self

    def transform(self, texts):
        matrices = [transformer.transform(texts) for _, transformer in self.transformers]
        return sparse.hstack(matrices).tocsr()


@dataclass
class ThresholdedTextModel:
    name: str
    labels: list[str]
    vectorizer: Any
    classifiers: dict[str, LogisticRegression | str | None]
    thresholds: dict[str, float]
    min_labels: int
    max_labels: int

    def _score_matrix(self, texts: pd.Series) -> np.ndarray:
        x_eval = self.vectorizer.transform(texts.fillna("").astype(str))
        scores = np.zeros((x_eval.shape[0], len(self.labels)), dtype=float)
        for idx, label in enumerate(self.labels):
            clf = self.classifiers.get(label)
            if clf == "always_one":
                scores[:, idx] = 1.0
            elif clf is not None:
                scores[:, idx] = clf.predict_proba(x_eval)[:, 1]
        return scores

    def predict_matrix(self, texts: pd.Series) -> np.ndarray:
        scores = self._score_matrix(texts)
        pred = np.zeros_like(scores, dtype=int)
        thresholds = np.array([self.thresholds.get(label, 0.5) for label in self.labels])
        pred[scores >= thresholds] = 1

        for row_idx in range(scores.shape[0]):
            order = np.argsort(scores[row_idx])[::-1]
            positive_count = int(pred[row_idx].sum())
            if positive_count < self.min_labels:
                pred[row_idx, order[: self.min_labels]] = 1
            if int(pred[row_idx].sum()) > self.max_labels:
                keep = set(order[: self.max_labels])
                for label_idx in range(pred.shape[1]):
                    if label_idx not in keep:
                        pred[row_idx, label_idx] = 0
        return pred

    def predict_labels_and_confidence(self, texts: pd.Series) -> tuple[list[list[str]], list[float]]:
        scores = self._score_matrix(texts)
        pred = self.predict_matrix(texts)
        labels_out: list[list[str]] = []
        confidences: list[float] = []
        for row_idx in range(pred.shape[0]):
            selected = [idx for idx, value in enumerate(pred[row_idx]) if value == 1]
            selected = sorted(selected, key=lambda idx: scores[row_idx, idx], reverse=True)
            labels_out.append([self.labels[idx] for idx in selected])
            confidences.append(float(np.mean([scores[row_idx, idx] for idx in selected])) if selected else 0.0)
        return labels_out, confidences


def load_split(path: Path, limit: int | None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["call_transcript"] = df["call_transcript"].fillna("").astype(str)
    df["label_list"] = df["call_code"].apply(split_labels)
    df = df[df["label_list"].apply(len) > 0].copy()
    if limit is not None:
        df = df.head(limit).copy()
    return df


def build_vectorizer(config_name: str, max_features: int):
    if config_name == "count_word":
        return CountVectorizer(
            lowercase=True,
            ngram_range=(1, 2),
            min_df=1,
            max_features=max_features,
        )
    if config_name == "tfidf_word":
        return TfidfVectorizer(
            lowercase=True,
            ngram_range=(1, 2),
            min_df=1,
            max_features=max_features,
            sublinear_tf=True,
            norm="l2",
        )
    if config_name == "tfidf_word_char":
        word_features = max(1000, int(max_features * 0.65))
        char_features = max(1000, max_features - word_features)
        return FeatureUnionLite(
            [
                (
                    "word",
                    TfidfVectorizer(
                        lowercase=True,
                        ngram_range=(1, 2),
                        min_df=1,
                        max_features=word_features,
                        sublinear_tf=True,
                        norm="l2",
                    ),
                ),
                (
                    "char",
                    TfidfVectorizer(
                        lowercase=True,
                        analyzer="char_wb",
                        ngram_range=(3, 5),
                        min_df=1,
                        max_features=char_features,
                        sublinear_tf=True,
                        norm="l2",
                    ),
                ),
            ]
        )
    raise ValueError(f"Unknown config_name={config_name!r}")


def train_model(
    name: str,
    vectorizer_name: str,
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    labels: list[str],
    max_features: int,
    min_labels: int,
    max_labels: int,
) -> ThresholdedTextModel:
    vectorizer = build_vectorizer(vectorizer_name, max_features=max_features)
    x_train = vectorizer.fit_transform(train_df["call_transcript"])

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

    model = ThresholdedTextModel(
        name=name,
        labels=labels,
        vectorizer=vectorizer,
        classifiers=classifiers,
        thresholds={label: 0.5 for label in labels},
        min_labels=min_labels,
        max_labels=max_labels,
    )
    model.thresholds = tune_thresholds(model, valid_df)
    return model


def train_model_with_thresholds(
    name: str,
    vectorizer_name: str,
    train_df: pd.DataFrame,
    labels: list[str],
    max_features: int,
    min_labels: int,
    max_labels: int,
    thresholds: dict[str, float],
) -> ThresholdedTextModel:
    vectorizer = build_vectorizer(vectorizer_name, max_features=max_features)
    x_train = vectorizer.fit_transform(train_df["call_transcript"])

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

    return ThresholdedTextModel(
        name=name,
        labels=labels,
        vectorizer=vectorizer,
        classifiers=classifiers,
        thresholds=thresholds,
        min_labels=min_labels,
        max_labels=max_labels,
    )


def tune_thresholds(model: ThresholdedTextModel, valid_df: pd.DataFrame) -> dict[str, float]:
    mlb = MultiLabelBinarizer(classes=model.labels)
    mlb.fit([model.labels])
    y_valid = mlb.transform(valid_df["label_list"])
    scores = model._score_matrix(valid_df["call_transcript"])
    thresholds: dict[str, float] = {}

    candidates = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]
    for idx, label in enumerate(model.labels):
        if y_valid[:, idx].sum() == 0:
            thresholds[label] = 0.50
            continue
        best_threshold = 0.50
        best_f1 = -1.0
        for threshold in candidates:
            pred = (scores[:, idx] >= threshold).astype(int)
            score = f1_score(y_valid[:, idx], pred, zero_division=0)
            if score > best_f1:
                best_f1 = score
                best_threshold = threshold
        thresholds[label] = best_threshold
    return thresholds


def evaluate(model: ThresholdedTextModel, eval_df: pd.DataFrame, split_name: str) -> dict[str, Any]:
    mlb = MultiLabelBinarizer(classes=model.labels)
    mlb.fit([model.labels])
    y_true = mlb.transform(eval_df["label_list"])
    y_pred = model.predict_matrix(eval_df["call_transcript"])
    predicted_labels, _ = model.predict_labels_and_confidence(eval_df["call_transcript"])

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
        target_names=model.labels,
        output_dict=True,
        zero_division=0,
    )
    return {
        "model": model.name,
        "split": split_name,
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


def write_metrics_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = [
        "model",
        "split",
        "eval_rows",
        "subset_accuracy",
        "exact_match_rate",
        "avg_jaccard",
        "micro_f1",
        "macro_f1",
        "weighted_f1",
        "hamming_loss",
    ]
    pd.DataFrame([{col: row[col] for col in columns} for row in rows]).to_csv(path, index=False)


def display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(ROOT))
    except ValueError:
        return str(resolved)


def write_report(path: Path, rows: list[dict[str, Any]], best_model_name: str) -> None:
    lines = [
        "# CallCenterEN Fine-Tuned BoW/TF-IDF Experiment",
        "",
        f"Best model selected by validation micro-F1 then Jaccard: `{best_model_name}`",
        "",
        "| Model | Split | Rows | Exact match | Avg Jaccard | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['model']} | {row['split']} | {row['eval_rows']:,} | "
            f"{row['exact_match_rate']:.4f} | {row['avg_jaccard']:.4f} | "
            f"{row['micro_f1']:.4f} | {row['macro_f1']:.4f} | "
            f"{row['weighted_f1']:.4f} | {row['hamming_loss']:.4f} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def apply_to_15k(
    model: ThresholdedTextModel,
    candidate_csv: Path,
    pseudo_label_csv: Path,
    output_csv: Path,
) -> pd.DataFrame:
    candidates = pd.read_csv(candidate_csv)
    pseudo = pd.read_csv(pseudo_label_csv)
    pseudo["pseudo_label_confidence"] = pseudo["pseudo_label_confidence"].apply(as_float)
    pseudo["should_use_for_training"] = pseudo["should_use_for_training"].apply(as_bool)
    pseudo = pseudo.rename(
        columns={
            "pseudo_call_code": "pseudo_call_code_existing",
            "pseudo_label_confidence": "pseudo_label_confidence_existing",
            "rationale": "pseudo_label_rationale",
        }
    )

    labels, confidences = model.predict_labels_and_confidence(candidates["text"].fillna("").astype(str))
    output = candidates.merge(
        pseudo[
            [
                "text_hash",
                "pseudo_call_code_existing",
                "pseudo_label_confidence_existing",
                "should_use_for_training",
                "pseudo_label_rationale",
            ]
        ],
        on="text_hash",
        how="left",
    )
    output["dataset_name"] = "callcenteren"
    output["call_transcript"] = output["text"].fillna("").astype(str)
    output["model_call_code"] = [", ".join(row) for row in labels]
    output["model_call_code_confidence"] = [round(value, 4) for value in confidences]
    output["model_name"] = model.name
    output["has_existing_pseudo_label"] = output["pseudo_call_code_existing"].notna()

    columns = [
        "dataset_name",
        "external_id",
        "text_hash",
        "source_zip",
        "source_entry",
        "source_domain",
        "call_direction",
        "call_transcript",
        "audio_duration",
        "confidence",
        "word_count",
        "char_count",
        "pii_token_count",
        "pii_types",
        "model_call_code",
        "model_call_code_confidence",
        "model_name",
        "has_existing_pseudo_label",
        "pseudo_call_code_existing",
        "pseudo_label_confidence_existing",
        "should_use_for_training",
        "pseudo_label_rationale",
    ]
    output[columns].to_csv(output_csv, index=False)
    return output[columns]


def write_prediction_summary(path: Path, predictions: pd.DataFrame) -> None:
    label_counts: Counter[str] = Counter()
    for value in predictions["model_call_code"]:
        label_counts.update(split_labels(value))

    summary = {
        "rows": int(len(predictions)),
        "rows_with_existing_pseudo_label": int(predictions["has_existing_pseudo_label"].sum()),
        "avg_model_call_code_confidence": round(float(predictions["model_call_code_confidence"].mean()), 4),
        "domain_distribution": predictions["source_domain"].fillna("unknown").value_counts().to_dict(),
        "direction_distribution": predictions["call_direction"].fillna("unknown").value_counts().to_dict(),
        "top_model_call_codes": label_counts.most_common(30),
    }
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--split-dir", type=Path, default=DEFAULT_SPLIT_DIR)
    parser.add_argument("--candidate-csv", type=Path, default=DEFAULT_CANDIDATE_CSV)
    parser.add_argument("--pseudo-label-csv", type=Path, default=DEFAULT_PSEUDO_LABEL_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit-train", type=int, default=None)
    parser.add_argument("--limit-eval", type=int, default=None)
    parser.add_argument("--limit-apply", type=int, default=None)
    parser.add_argument("--max-features", type=int, default=70000)
    parser.add_argument("--min-labels", type=int, default=2)
    parser.add_argument("--max-labels", type=int, default=5)
    parser.add_argument("--refit-train-valid", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    train_df = load_split(args.split_dir / "callcenteren_train.csv", args.limit_train)
    valid_df = load_split(args.split_dir / "callcenteren_valid.csv", args.limit_eval)
    test_df = load_split(args.split_dir / "callcenteren_test.csv", args.limit_eval)

    labels = sorted(set(label for labels in train_df["label_list"] for label in labels))
    for df in [valid_df, test_df]:
        df["label_list"] = df["label_list"].apply(lambda values: [value for value in values if value in labels])
        df.drop(df[df["label_list"].apply(len) == 0].index, inplace=True)

    configs = [
        ("count_word_lr_threshold", "count_word"),
        ("tfidf_word_lr_threshold", "tfidf_word"),
        ("tfidf_word_char_lr_threshold", "tfidf_word_char"),
    ]

    models: list[ThresholdedTextModel] = []
    model_config_by_name: dict[str, str] = {}
    metrics: list[dict[str, Any]] = []
    for model_name, vectorizer_name in configs:
        model_config_by_name[model_name] = vectorizer_name
        model = train_model(
            name=model_name,
            vectorizer_name=vectorizer_name,
            train_df=train_df,
            valid_df=valid_df,
            labels=labels,
            max_features=args.max_features,
            min_labels=args.min_labels,
            max_labels=args.max_labels,
        )
        models.append(model)
        metrics.append(evaluate(model, valid_df, "valid"))
        metrics.append(evaluate(model, test_df, "test"))

    valid_metrics = [row for row in metrics if row["split"] == "valid"]
    best_valid = sorted(valid_metrics, key=lambda row: (row["micro_f1"], row["avg_jaccard"]), reverse=True)[0]
    best_model = next(model for model in models if model.name == best_valid["model"])

    if args.refit_train_valid:
        refit_train_df = pd.concat([train_df, valid_df], ignore_index=True)
        best_model = train_model_with_thresholds(
            name=f"{best_model.name}_refit_train_valid",
            vectorizer_name=model_config_by_name[best_valid["model"]],
            train_df=refit_train_df,
            labels=labels,
            max_features=args.max_features,
            min_labels=args.min_labels,
            max_labels=args.max_labels,
            thresholds=best_model.thresholds,
        )
        metrics.append(evaluate(best_model, test_df, "test_refit_train_valid"))

    metrics_json = args.output_dir / "callcenteren_finetune_metrics.json"
    metrics_csv = args.output_dir / "callcenteren_finetune_metrics.csv"
    report_md = args.output_dir / "callcenteren_finetune_report.md"
    model_path = args.output_dir / "callcenteren_best_finetuned_model.pkl"
    predictions_csv = args.output_dir / "callcenteren_15k_with_model_callcodes.csv"
    prediction_summary_json = args.output_dir / "callcenteren_15k_model_callcode_summary.json"

    metrics_json.write_text(
        json.dumps(
            {
                "labels": labels,
                "best_model": best_model.name,
                "best_model_thresholds": best_model.thresholds,
                "metrics": metrics,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_metrics_csv(metrics_csv, metrics)
    write_report(report_md, metrics, best_model.name)
    joblib.dump(best_model, model_path)

    candidate_csv = args.candidate_csv
    if args.limit_apply is not None:
        tmp = pd.read_csv(args.candidate_csv).head(args.limit_apply)
        candidate_csv = args.output_dir / "_limited_candidates_for_apply.csv"
        tmp.to_csv(candidate_csv, index=False)

    predictions = apply_to_15k(best_model, candidate_csv, args.pseudo_label_csv, predictions_csv)
    write_prediction_summary(prediction_summary_json, predictions)
    if args.limit_apply is not None and candidate_csv.exists():
        candidate_csv.unlink()

    print(f"Best model: {best_model.name}")
    for row in metrics:
        print(
            f"{row['model']} {row['split']}: "
            f"micro-F1={row['micro_f1']:.4f}, macro-F1={row['macro_f1']:.4f}, "
            f"jaccard={row['avg_jaccard']:.4f}, exact={row['exact_match_rate']:.4f}"
        )
    print(f"Wrote {display_path(metrics_csv)}")
    print(f"Wrote {display_path(report_md)}")
    print(f"Wrote {display_path(model_path)}")
    print(f"Wrote {display_path(predictions_csv)} ({len(predictions):,} rows)")


if __name__ == "__main__":
    main()
