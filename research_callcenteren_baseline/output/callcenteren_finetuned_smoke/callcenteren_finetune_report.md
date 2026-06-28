# CallCenterEN Fine-Tuned BoW/TF-IDF Experiment

Best model selected by validation micro-F1 then Jaccard: `tfidf_word_char_lr_threshold`

| Model | Split | Rows | Exact match | Avg Jaccard | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| count_word_lr_threshold | valid | 40 | 0.1000 | 0.6171 | 0.7413 | 0.2992 | 0.7274 | 0.1156 |
| count_word_lr_threshold | test | 40 | 0.1500 | 0.5888 | 0.7108 | 0.2503 | 0.7315 | 0.1297 |
| tfidf_word_lr_threshold | valid | 40 | 0.0750 | 0.5792 | 0.7133 | 0.3353 | 0.7540 | 0.1344 |
| tfidf_word_lr_threshold | test | 40 | 0.0000 | 0.5249 | 0.6667 | 0.2439 | 0.7231 | 0.1547 |
| tfidf_word_char_lr_threshold | valid | 40 | 0.1250 | 0.6558 | 0.7724 | 0.3700 | 0.7688 | 0.1031 |
| tfidf_word_char_lr_threshold | test | 40 | 0.0500 | 0.5635 | 0.7014 | 0.2406 | 0.7212 | 0.1344 |
