# External Baseline Dataset Analysis: CallCenterEN

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
| Records | 23,447 | 3,000 |
| Avg transcript length, chars | 897.2545 | 3,890.2737 |
| Median transcript length, chars | 956 | 3,843.0 |
| Avg word count | 151.0447 | 644.6783 |
| Avg call duration, seconds | 272.8059 | 348.4023 |
| Task-specific labels | `call_code` | Not available |
| ASR confidence | Not available | 0.9534 |
| Avg PII redaction tokens | Not directly encoded as tokens | 58.8953 |

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

| Item | Count |
|---|---:|
| `insurance` | 2,694 |
| `customer_service` | 306 |

### Direction Distribution

| Item | Count |
|---|---:|
| `outbound` | 1,666 |
| `inbound` | 1,334 |

### Top PII Types

| Item | Count |
|---|---:|
| `PERSON_NAME` | 2,976 |
| `OCCUPATION` | 2,861 |
| `LOCATION` | 2,801 |
| `DURATION` | 2,700 |
| `ORGANIZATION` | 2,620 |
| `DATE_OF_BIRTH` | 2,555 |
| `MONEY_AMOUNT` | 1,944 |
| `DATE_INTERVAL` | 822 |
| `MARITAL_STATUS` | 781 |
| `PHONE_NUMBER` | 731 |

## Primary Label Space

The thesis classifier is trained and evaluated using the primary dataset's
`call_code` label space. The most frequent labels are:

| Item | Count |
|---|---:|
| `OPENING` | 9,959 |
| `PRODUCT_PITCH` | 6,034 |
| `HARD_REJECTION` | 4,633 |
| `NEEDS_ANALYSIS` | 3,287 |
| `FEE_DISCUSSION` | 2,583 |
| `SOFT_REJECTION` | 2,432 |
| `PASSIVE_AGREEMENT` | 2,410 |
| `ANGRY_OUTBURST` | 2,290 |
| `SUSPICIOUS_PROBING` | 1,950 |
| `DEFENSIVE_POSTURE` | 1,890 |
| `FOLLOW_UP_EMAIL_REQUESTED` | 1,876 |
| `CURIOUS_EXPLORATION` | 1,766 |
| `SUDDEN_HANG_UP` | 1,548 |
| `ANNOYED_SIGHING` | 1,477 |
| `OVERWHELMED_CONFUSION` | 1,337 |

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
