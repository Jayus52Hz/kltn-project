# CallCenterEN Fine-Tuned BoW/TF-IDF Experiment

Best model selected by validation micro-F1 then Jaccard: `count_word_lr_threshold`

| Model | Split | Rows | Exact match | Avg Jaccard | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| count_word_lr_threshold | valid | 315 | 0.1302 | 0.6144 | 0.7411 | 0.2210 | 0.7273 | 0.0662 |
| count_word_lr_threshold | test | 347 | 0.1412 | 0.5943 | 0.7241 | 0.1933 | 0.7128 | 0.0689 |
| tfidf_word_lr_threshold | valid | 315 | 0.1111 | 0.6090 | 0.7386 | 0.1964 | 0.7350 | 0.0703 |
| tfidf_word_lr_threshold | test | 347 | 0.1326 | 0.6049 | 0.7360 | 0.2080 | 0.7426 | 0.0698 |
| tfidf_word_char_lr_threshold | valid | 315 | 0.1302 | 0.6121 | 0.7403 | 0.2136 | 0.7410 | 0.0696 |
| tfidf_word_char_lr_threshold | test | 347 | 0.1326 | 0.5983 | 0.7312 | 0.2147 | 0.7396 | 0.0706 |
