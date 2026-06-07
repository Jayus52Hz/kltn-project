# Auxiliary Pseudo-Label BoW Experiment

## Setup

The experiment compares a supervised BoW baseline trained only on the primary
thesis dataset against the same BoW classifier augmented with AI-assisted
pseudo-labeled CallCenterEN samples. The evaluation sets remain the original
validation and test splits from the primary dataset.

Pseudo-labeled auxiliary rows used: 294

## Pseudo-Label Distribution

| Pseudo-label | Count |
|---|---:|
| `OPENING` | 293 |
| `NEEDS_ANALYSIS` | 288 |
| `PRODUCT_PITCH` | 211 |
| `FEE_DISCUSSION` | 190 |
| `PASSIVE_AGREEMENT` | 60 |
| `OBJECTION_HANDLING` | 18 |
| `CURIOUS_EXPLORATION` | 17 |
| `SOFT_REJECTION` | 16 |
| `WARM_LEAD` | 11 |
| `DEFENSIVE_POSTURE` | 8 |
| `ACTIVE_LISTENING` | 8 |
| `MISUNDERSTANDING` | 8 |
| `OVERWHELMED_CONFUSION` | 7 |
| `ENTHUSIASTIC_AGREEMENT` | 6 |
| `SUDDEN_HANG_UP` | 5 |
| `COMPETITOR_COMPARISON` | 4 |
| `SUSPICIOUS_PROBING` | 3 |
| `SUCCESSFUL_SALE` | 3 |
| `FOLLOW_UP_EMAIL_REQUESTED` | 2 |
| `ANGRY_OUTBURST` | 1 |

## Validation Results

| Model | Train rows | Eval rows | Subset accuracy | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| M0_primary_only_bow | 13,857 | 1,732 | 0.2610 | 0.7298 | 0.6501 | 0.7316 | 0.0691 |
| M3_primary_plus_callcenteren_pseudo_bow | 14,151 | 1,732 | 0.2604 | 0.7300 | 0.6494 | 0.7318 | 0.0690 |

## Test Results

| Model | Train rows | Eval rows | Subset accuracy | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| M0_primary_only_bow | 13,857 | 1,733 | 0.2539 | 0.7251 | 0.6397 | 0.7260 | 0.0703 |
| M3_primary_plus_callcenteren_pseudo_bow | 14,151 | 1,733 | 0.2539 | 0.7261 | 0.6413 | 0.7270 | 0.0700 |

## Interpretation

If the auxiliary model improves macro-F1 or per-label F1, the result supports
the claim that CallCenterEN can contribute useful real-world call-center
language signals. If the auxiliary model does not improve the primary test set,
the result is still methodologically valid because pseudo-label noise and domain
shift are expected risks in weak supervision.
