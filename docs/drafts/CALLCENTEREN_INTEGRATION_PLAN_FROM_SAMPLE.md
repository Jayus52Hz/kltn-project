# Plan đưa CallCenterEN vào đồ án từ mẫu dữ liệu đã generate

## 1. Nhận xét từ dữ liệu mẫu

Phần CallCenterEN đã generate hiện có bộ `callcenteren_15k_candidate.csv` gồm 15.000 dòng được chọn từ 95.939 raw rows, sau deduplicate còn 90.432 unique rows và 74.226 candidate rows. Bộ 15k có các trường chính: `external_id`, `source_zip`, `source_domain`, `call_direction`, `text`, `audio_duration`, `confidence`, `word_count`, `char_count`, `pii_token_count`, `pii_types`, `text_hash`.

Đặc trưng dữ liệu:

- Đây là transcript call-center thật hoặc gần thực tế, đã redacted PII bằng token như `[PERSON_NAME]`, `[ORGANIZATION]`, `[PHONE_NUMBER]`.
- Có metadata hữu ích để đánh giá chất lượng: ASR confidence, thời lượng cuộc gọi, số từ, số ký tự, domain và inbound/outbound.
- Tập 15k hiện có inbound 11.029 dòng, outbound 3.971 dòng, avg duration khoảng 349,53 giây, avg confidence khoảng 0,9128, avg transcript length khoảng 3.746 ký tự.
- Dữ liệu dài hơn dataset chính khá nhiều, phù hợp để làm external call-center corpus.

Phần pseudo-label hiện tại đang sinh ra các cột:

- `pseudo_call_code`: nhãn multi-label theo taxonomy `call_code` của đồ án;
- `pseudo_label_confidence`: độ tin cậy của label;
- `should_use_for_training`: cờ có nên dùng cho training phụ;
- `rationale`: giải thích ngắn cho nhãn.

Snapshot hiện tại đã label 1.175/15.000 dòng, trong đó 1.143 dòng có confidence >= 0,80, chưa thấy label ngoài taxonomy. Các label xuất hiện nhiều gồm `OPENING`, `NEEDS_ANALYSIS`, `PRODUCT_PITCH`, `FEE_DISCUSSION`, `CURIOUS_EXPLORATION`.

## 2. Ý nghĩa đối với đồ án

CallCenterEN không giống dataset chính ở tầng nghiệp vụ vì không có customer, offer, campaign và quan hệ telesales nội bộ. Tuy nhiên, nó rất có giá trị để chứng minh rằng thiết kế dataset chính có cơ sở thực tế: đều xoay quanh transcript cuộc gọi, thời lượng cuộc gọi, tín hiệu PII, hành vi hội thoại và phân loại nội dung cuộc gọi.

Vì vậy, cách đưa vào đồ án hợp lý là xây dựng một nhánh dữ liệu phụ có kiểm soát:

- dataset chính vẫn là nguồn phục vụ bài toán AGI Telesales;
- CallCenterEN là external call-center corpus để đối chiếu, đánh giá độ tin cậy và thử nghiệm pseudo-label;
- pseudo-label chỉ được gọi là weak label/pseudo-label, không gọi là ground truth.

## 3. Plan triển khai vào hệ thống

### Bước 1: Chuẩn hóa dữ liệu CallCenterEN

Tạo một luồng file-based ingestion cho CallCenterEN, không đi qua MongoDB/Debezium vì nguồn này không phải operational CDC source.

Các bảng đề xuất:

- `lakehouse.bronze_external.callcenteren_raw`: lưu gần nguyên bản từ CSV, gồm transcript, source, confidence, duration, hash.
- `lakehouse.silver_external.callcenteren_clean`: parse kiểu dữ liệu, deduplicate theo `text_hash`, chuẩn hóa domain/direction, tính length/quality fields.
- `lakehouse.silver_external.callcenteren_pseudo_labels`: lưu `pseudo_call_code`, confidence, training flag và rationale.
- `lakehouse.gold_external.dataset_comparison_metrics`: metric so sánh dataset chính và CallCenterEN.

### Bước 2: Làm data quality và profiling

Sinh các thống kê bắt buộc:

- số dòng raw/unique/candidate/selected;
- phân phối domain và inbound/outbound;
- avg/median duration;
- avg/median word count và char count;
- phân phối ASR confidence;
- PII token/type distribution;
- tỷ lệ pseudo-label hợp lệ, confidence >= 0,80, label ngoài taxonomy nếu có.

Kết quả dùng được cho cả báo cáo và phần bảo vệ.

### Bước 3: So sánh với dataset chính

Tạo bảng so sánh:

| Tiêu chí | Dataset chính | CallCenterEN |
|---|---|---|
| Vai trò | Dataset nghiệp vụ chính | External call-center corpus |
| Dữ liệu | customer, offer, call_logs, transcript | transcript, duration, ASR confidence, PII token |
| Nhãn | `call_code` gốc/phục vụ training | pseudo `call_code` |
| Mục đích | ETL, NLP inference, BI telesales | baseline, đối chiếu, auxiliary training |
| Dùng làm test set | Có | Không |

### Bước 4: Kiểm tra pseudo-label

Khi job 15k chạy xong:

1. Chạy lại `inspect_pseudo_label_quality.py`.
2. Lọc dòng `should_use_for_training = true` và confidence >= 0,80.
3. Review thủ công 100-200 dòng đại diện theo domain/direction/label.
4. Loại dòng có label quá mơ hồ, confidence thấp hoặc rationale không khớp transcript.
5. Chốt một tập auxiliary clean để dùng cho thí nghiệm.

### Bước 5: Thí nghiệm mô hình

Thiết kế tối thiểu 2 cấu hình:

- M0: train bằng dataset chính.
- M1: train bằng dataset chính + CallCenterEN pseudo-label đã lọc.

Nguyên tắc đánh giá:

- valid/test vẫn lấy từ dataset chính;
- không đưa CallCenterEN vào test set;
- nếu M1 tốt hơn, chứng minh dữ liệu ngoài giúp bổ sung tín hiệu hội thoại;
- nếu M1 không tốt hơn, vẫn có giá trị vì chỉ ra domain shift và noise của weak supervision.

### Bước 6: Đưa vào báo cáo

Đưa CallCenterEN vào Chương 2 hoặc Chương 3 như một nhánh dữ liệu hỗ trợ:

- mô tả nguồn CallCenterEN và lý do chọn;
- trình bày quy trình lọc 15k candidate;
- trình bày bảng so sánh dataset chính vs CallCenterEN;
- trình bày pseudo-label pipeline và quality control;
- trình bày thí nghiệm M0/M1 nếu có kết quả;
- kết luận rằng CallCenterEN giúp tăng độ tin cậy dataset nhưng không thay thế dataset chính.

## 4. Câu mô tả ngắn để dùng với giảng viên

Em đã lấy một phần CallCenterEN đã generate để kiểm tra cấu trúc dữ liệu. Dữ liệu này có transcript call-center, thời lượng cuộc gọi, ASR confidence, domain, inbound/outbound và PII token, nên phù hợp để làm corpus đối chiếu cho dataset chính. Em dự định đưa CallCenterEN vào như một luồng phụ trong Lakehouse để profiling, so sánh với dataset chính và thử nghiệm pseudo-label, nhưng không dùng nó làm ground truth hoặc test set chính.
