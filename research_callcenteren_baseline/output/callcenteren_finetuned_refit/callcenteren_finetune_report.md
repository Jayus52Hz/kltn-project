# CallCenterEN Fine-Tuned BoW/TF-IDF Experiment

Best model selected by validation micro-F1 then Jaccard: `count_word_lr_threshold_refit_train_valid`

| Model | Split | Rows | Exact match | Avg Jaccard | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| count_word_lr_threshold | valid | 315 | 0.1079 | 0.6068 | 0.7368 | 0.2338 | 0.7355 | 0.0697 |
| count_word_lr_threshold | test | 347 | 0.1095 | 0.5861 | 0.7187 | 0.2044 | 0.7187 | 0.0727 |
| tfidf_word_lr_threshold | valid | 315 | 0.0508 | 0.5854 | 0.7236 | 0.2366 | 0.7546 | 0.0811 |
| tfidf_word_lr_threshold | test | 347 | 0.0576 | 0.5821 | 0.7207 | 0.2411 | 0.7601 | 0.0808 |
| tfidf_word_char_lr_threshold | valid | 315 | 0.0698 | 0.5889 | 0.7230 | 0.2458 | 0.7592 | 0.0811 |
| tfidf_word_char_lr_threshold | test | 347 | 0.0778 | 0.5721 | 0.7102 | 0.2267 | 0.7554 | 0.0837 |
| count_word_lr_threshold_refit_train_valid | test_refit_train_valid | 347 | 0.1268 | 0.5819 | 0.7150 | 0.1948 | 0.7177 | 0.0737 |
