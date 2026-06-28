# CallCenterEN Fine-Tuned BoW/TF-IDF Experiment

Best model selected by validation micro-F1 then Jaccard: `count_word_lr_threshold`

| Model | Split | Rows | Exact match | Avg Jaccard | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| count_word_lr_threshold | valid | 315 | 0.1079 | 0.6051 | 0.7347 | 0.2339 | 0.7370 | 0.0710 |
| count_word_lr_threshold | test | 347 | 0.1095 | 0.5814 | 0.7141 | 0.2016 | 0.7178 | 0.0744 |
| tfidf_word_lr_threshold | valid | 315 | 0.0286 | 0.5563 | 0.7001 | 0.2736 | 0.7633 | 0.0937 |
| tfidf_word_lr_threshold | test | 347 | 0.0259 | 0.5477 | 0.6935 | 0.2463 | 0.7626 | 0.0942 |
| tfidf_word_char_lr_threshold | valid | 315 | 0.0571 | 0.5634 | 0.6994 | 0.2716 | 0.7644 | 0.0930 |
| tfidf_word_char_lr_threshold | test | 347 | 0.0461 | 0.5393 | 0.6836 | 0.2364 | 0.7535 | 0.0962 |
