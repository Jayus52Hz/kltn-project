# Pseudo-Label Pilot Summary

## Configuration

- Provider: Google AI Studio generateContent API
- Model: `gemma-4-31b-it`
- RPM constraint: 15 requests per minute
- Execution sleep: 4.2 seconds between requests
- Input file: `92k-real-world-call-center-scripts-english/prepared_subset/auxiliary_training_candidate.csv`
- Target pseudo-label rows: 300
- Output file: `research_callcenteren_baseline/output/pseudo_labels_gemini.csv`

## Result

| Metric | Value |
|---|---:|
| Generated pseudo-label rows | 300 |
| Rows marked usable for training | 295 |
| Rows excluded by training filter | 5 |
| Rows finally used in BoW auxiliary experiment | 294 |

## Label Distribution

| Label | Count |
|---|---:|
| `OPENING` | 299 |
| `NEEDS_ANALYSIS` | 291 |
| `PRODUCT_PITCH` | 213 |
| `FEE_DISCUSSION` | 190 |
| `PASSIVE_AGREEMENT` | 60 |
| `OBJECTION_HANDLING` | 18 |
| `CURIOUS_EXPLORATION` | 17 |
| `SOFT_REJECTION` | 16 |
| `WARM_LEAD` | 11 |
| `ACTIVE_LISTENING` | 9 |
| `DEFENSIVE_POSTURE` | 8 |
| `MISUNDERSTANDING` | 8 |
| `OVERWHELMED_CONFUSION` | 7 |
| `ENTHUSIASTIC_AGREEMENT` | 6 |
| `SUDDEN_HANG_UP` | 5 |
| `COMPETITOR_COMPARISON` | 4 |
| `SUSPICIOUS_PROBING` | 3 |
| `SUCCESSFUL_SALE` | 3 |
| `FOLLOW_UP_EMAIL_REQUESTED` | 2 |
| `STALLING_FOR_TIME` | 1 |

## BoW Auxiliary Experiment

Evaluation remains on the original thesis validation/test sets. CallCenterEN
pseudo-labels are used only as auxiliary training rows.

| Split | Model | Micro-F1 | Macro-F1 | Hamming loss |
|---|---|---:|---:|---:|
| Validation | M0 primary only | 0.7298 | 0.6501 | 0.0691 |
| Validation | M3 primary + pseudo CallCenterEN | 0.7300 | 0.6494 | 0.0690 |
| Test | M0 primary only | 0.7251 | 0.6397 | 0.0703 |
| Test | M3 primary + pseudo CallCenterEN | 0.7261 | 0.6413 | 0.0700 |

## Interpretation

The 300-row pseudo-label pilot provides a concrete auxiliary-training bridge
between CallCenterEN and the thesis classifier. The effect on the original test
set is positive but small: micro-F1 increases from 0.7251 to 0.7261 and macro-F1
increases from 0.6397 to 0.6413.

This should be reported as a pilot result rather than a definitive model
improvement. It supports the claim that CallCenterEN can participate in model
training as a weakly labeled auxiliary corpus, while the primary thesis dataset
remains the only source of task-specific ground truth.
