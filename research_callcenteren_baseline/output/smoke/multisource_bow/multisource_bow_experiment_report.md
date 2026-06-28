# Multi-Source BoW Experiment

Label taxonomy size: 32

| Model | Train dataset | Eval dataset | Train rows | Eval rows | Exact match | Avg Jaccard | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| M0_primary_bow | primary_telesales | primary_valid | 80 | 40 | 0.0000 | 0.3550 | 0.5176 | 0.2193 | 0.4374 | 0.0961 |
| M0_primary_bow | primary_telesales | primary_test | 80 | 40 | 0.0000 | 0.2632 | 0.4032 | 0.1514 | 0.3340 | 0.1156 |
| M1_primary_to_callcenteren | primary_telesales | callcenteren_valid | 80 | 40 | 0.0000 | 0.2260 | 0.3577 | 0.0467 | 0.2904 | 0.1234 |
| M1_primary_to_callcenteren | primary_telesales | callcenteren_test | 80 | 40 | 0.0000 | 0.2657 | 0.4000 | 0.0595 | 0.3412 | 0.1078 |
| M2_callcenteren_bow | callcenteren | callcenteren_valid | 80 | 40 | 0.1000 | 0.6038 | 0.7287 | 0.1026 | 0.6617 | 0.0547 |
| M2_callcenteren_bow | callcenteren | callcenteren_test | 80 | 40 | 0.1000 | 0.6079 | 0.7393 | 0.1184 | 0.7093 | 0.0523 |
| M3_callcenteren_to_primary | callcenteren | primary_valid | 80 | 40 | 0.0000 | 0.2547 | 0.3892 | 0.0789 | 0.3025 | 0.1594 |
| M3_callcenteren_to_primary | callcenteren | primary_test | 80 | 40 | 0.0000 | 0.2070 | 0.3294 | 0.0712 | 0.2279 | 0.1781 |
| M4_combined_bow | primary_telesales+callcenteren | primary_valid | 160 | 40 | 0.0000 | 0.3132 | 0.4741 | 0.2081 | 0.4158 | 0.1109 |
| M4_combined_bow | primary_telesales+callcenteren | primary_test | 160 | 40 | 0.0000 | 0.2623 | 0.4000 | 0.1617 | 0.3400 | 0.1195 |
| M4_combined_bow | primary_telesales+callcenteren | callcenteren_valid | 160 | 40 | 0.1000 | 0.5652 | 0.6889 | 0.0998 | 0.6460 | 0.0656 |
| M4_combined_bow | primary_telesales+callcenteren | callcenteren_test | 160 | 40 | 0.1500 | 0.5925 | 0.7203 | 0.1053 | 0.6857 | 0.0570 |

## Interpretation Note

CallCenterEN pseudo-labels are treated as trusted labels for this experiment. Primary and CallCenterEN test splits remain separate, so combined training does not leak either test set into training.
