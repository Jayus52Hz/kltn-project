# Báo cáo triển khai CallCenterEN

## Mục tiêu

CallCenterEN được đưa vào đồ án như một nhánh dataset chính ngang hàng với dataset AGI Telesales hiện tại. Luồng mới phục vụ hai mục tiêu: xây dựng mô hình call_code riêng cho CallCenterEN và tạo các bảng Lakehouse để so sánh hai nguồn dữ liệu.

## Dữ liệu và pseudo-label

- Candidate dataset: 15.000 dòng CallCenterEN.
- Pseudo-label Gemini đã sinh trước khi dừng: 2.420 dòng.
- Dòng pseudo-label dùng được sau lọc mặc định: 2.340 dòng.
- Dòng đưa vào split huấn luyện sau merge/lọc: 2.260 dòng.
- Split deterministic theo `text_hash`:
  - Train: 1.598 dòng.
  - Valid: 315 dòng.
  - Test: 347 dòng.

## Thí nghiệm trước fine-tune

Model BoW đã train trên dataset chính khi đánh sang CallCenterEN cho kết quả thấp, cho thấy có lệch miền rõ rệt:

- Eval rows: 2.260.
- Exact match: 0,0004.
- Avg Jaccard: 0,2606.
- Micro-F1: 0,3970.
- Macro-F1: 0,1017.

Thí nghiệm multi-source M0-M4 đã được chạy để có bằng chứng so sánh, nhưng theo hướng chốt hiện tại không dùng combined model làm mô hình chính.

## Fine-tune model riêng cho CallCenterEN

Đã chạy fine-tune riêng cho CallCenterEN với các biến thể BoW/TF-IDF và threshold tuning theo validation. Model được chọn theo validation micro-F1 rồi Jaccard là `count_word_lr_threshold`.

| Model | Split | Rows | Exact match | Avg Jaccard | Micro-F1 | Macro-F1 | Weighted-F1 | Hamming loss |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| count_word_lr_threshold | valid | 315 | 0,1302 | 0,6144 | 0,7411 | 0,2210 | 0,7273 | 0,0662 |
| count_word_lr_threshold | test | 347 | 0,1412 | 0,5943 | 0,7241 | 0,1933 | 0,7128 | 0,0689 |
| tfidf_word_lr_threshold | test | 347 | 0,1326 | 0,6049 | 0,7360 | 0,2080 | 0,7426 | 0,0698 |
| tfidf_word_char_lr_threshold | test | 347 | 0,1326 | 0,5983 | 0,7312 | 0,2147 | 0,7396 | 0,0706 |

## Áp dụng model lên 15k CallCenterEN

Đã dùng model CallCenterEN đã fine-tune để sinh `model_call_code` cho toàn bộ 15.000 dòng.

- Tổng dòng: 15.000.
- Dòng có pseudo-label gốc từ Gemini: 2.340.
- Trung bình confidence của model call_code: 0,888.
- Phân bố domain:
  - medicare: 4.384.
  - insurance: 3.964.
  - home_service_telecom: 3.848.
  - automotive: 1.828.
  - customer_service: 493.
  - medical_equipment: 483.
- Phân bố direction:
  - inbound: 11.029.
  - outbound: 3.971.
- Top call_code dự đoán:
  - OPENING: 14.946.
  - NEEDS_ANALYSIS: 13.921.
  - FEE_DISCUSSION: 6.103.
  - PRODUCT_PITCH: 4.754.
  - CURIOUS_EXPLORATION: 3.283.
  - ACTIVE_LISTENING: 2.872.
  - PASSIVE_AGREEMENT: 1.729.
  - SUCCESSFUL_SALE: 1.584.

## Lakehouse/Docker

Docker stack đã được build và start thành công. Đã chạy Spark ETL CallCenterEN và ghi các bảng Iceberg/MinIO sau:

| Table | Rows |
|---|---:|
| lakehouse.bronze_external.callcenteren_raw | 15.000 |
| lakehouse.silver_external.callcenteren_clean | 15.000 |
| lakehouse.silver_external.callcenteren_labeled | 15.000 |
| lakehouse.gold_external.callcenteren_call_analytics | 8 |
| lakehouse.gold.dataset_profile_comparison | 2 |
| lakehouse.gold.call_code_distribution_comparison | 59 |
| lakehouse.gold.model_experiment_comparison | 6 |

Dataset profile comparison hiện có:

| Dataset | Rows | Avg duration | Avg word count | Avg char count | Avg PII token count |
|---|---:|---:|---:|---:|---:|
| callcenteren | 15.000 | 349,5284 | 639,8362 | 3.746,2027 | 55,5125 |
| primary_telesales | 23.447 | 272,8059 | 151,0650 | 898,6230 | null |

## Artifact chính

- `research_callcenteren_baseline/output/callcenteren_finetuned_max4/callcenteren_best_finetuned_model.pkl`
- `research_callcenteren_baseline/output/callcenteren_finetuned_max4/callcenteren_15k_with_model_callcodes.csv`
- `research_callcenteren_baseline/output/callcenteren_finetuned_max4/callcenteren_finetune_report.md`
- `research_callcenteren_baseline/output/callcenteren_finetuned_max4/callcenteren_15k_model_callcode_summary.json`
- `research_callcenteren_baseline/output/callcenteren_train.csv`
- `research_callcenteren_baseline/output/callcenteren_valid.csv`
- `research_callcenteren_baseline/output/callcenteren_test.csv`

## Kết luận

CallCenterEN đã có đầy đủ luồng riêng từ data preparation, split, fine-tune model, sinh call_code 15k, đến ghi bảng Bronze/Silver/Gold trong Lakehouse. Kết quả cho thấy dataset chính và CallCenterEN khác miền khá mạnh; vì vậy hướng tách thành hai model riêng là hợp lý hơn combined model ở giai đoạn này.
