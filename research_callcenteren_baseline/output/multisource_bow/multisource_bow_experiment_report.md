# Multi-Source BoW Experiment

Label taxonomy size: 32

| Model | Train dataset | Eval dataset | Train rows | Eval rows | Exact match | Avg Jaccard | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| M0_primary_bow | primary_telesales | primary_valid | 13,857 | 1,732 | 0.2633 | 0.6231 | 0.7309 | 0.6501 | 0.7326 | 0.0688 |
| M0_primary_bow | primary_telesales | primary_test | 13,857 | 1,733 | 0.2527 | 0.6163 | 0.7261 | 0.6407 | 0.7267 | 0.0699 |
| M1_primary_to_callcenteren | primary_telesales | callcenteren_valid | 13,857 | 315 | 0.0063 | 0.3608 | 0.5066 | 0.0932 | 0.5217 | 0.1158 |
| M1_primary_to_callcenteren | primary_telesales | callcenteren_test | 13,857 | 347 | 0.0058 | 0.3514 | 0.4953 | 0.0931 | 0.5140 | 0.1155 |
| M2_callcenteren_bow | callcenteren | callcenteren_valid | 1,598 | 315 | 0.1206 | 0.6116 | 0.7402 | 0.1726 | 0.7110 | 0.0563 |
| M2_callcenteren_bow | callcenteren | callcenteren_test | 1,598 | 347 | 0.1239 | 0.5929 | 0.7291 | 0.1589 | 0.7062 | 0.0570 |
| M3_callcenteren_to_primary | callcenteren | primary_valid | 1,598 | 1,732 | 0.0006 | 0.2187 | 0.3417 | 0.1490 | 0.3235 | 0.1928 |
| M3_callcenteren_to_primary | callcenteren | primary_test | 1,598 | 1,733 | 0.0000 | 0.2176 | 0.3405 | 0.1451 | 0.3181 | 0.1939 |
| M4_combined_bow | primary_telesales+callcenteren | primary_valid | 15,455 | 1,732 | 0.2564 | 0.6199 | 0.7283 | 0.6429 | 0.7304 | 0.0696 |
| M4_combined_bow | primary_telesales+callcenteren | primary_test | 15,455 | 1,733 | 0.2499 | 0.6139 | 0.7238 | 0.6389 | 0.7249 | 0.0708 |
| M4_combined_bow | primary_telesales+callcenteren | callcenteren_valid | 15,455 | 315 | 0.0635 | 0.5577 | 0.6957 | 0.2011 | 0.6997 | 0.0700 |
| M4_combined_bow | primary_telesales+callcenteren | callcenteren_test | 15,455 | 347 | 0.0836 | 0.5625 | 0.7032 | 0.1871 | 0.7101 | 0.0667 |

## Interpretation Note

CallCenterEN pseudo-labels are treated as trusted labels for this experiment. Primary and CallCenterEN test splits remain separate, so combined training does not leak either test set into training.
