# Trained BoW on CallCenterEN Pseudo-Labels

## Setup

- Model: `D:\Đồ án tốt nghiệp\NLP model\models\bow_model.pkl`
- Source rows after filter: 2,260
- Pseudo-label confidence threshold: 0.8
- Require `should_use_for_training`: True

## Metrics

| Metric | Value |
|---|---:|
| Exact-match accuracy | 0.0004 |
| Average Jaccard | 0.2606 |
| Micro-F1 | 0.3970 |
| Macro-F1 | 0.1017 |
| Weighted-F1 | 0.5107 |
| Hamming loss | 0.1817 |

## Interpretation Note

This evaluation treats CallCenterEN pseudo-labels as trusted labels for a quick
compatibility check. It measures how the already-trained thesis BoW model
transfers to the generated CallCenterEN branch without retraining.
