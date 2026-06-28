# Ghi chú chuẩn bị luồng CallCenterEN

## Bối cảnh

Đồ án xây dựng nền tảng Hybrid Data Lakehouse cho hệ thống AGI Telesales, xử lý dữ liệu khách hàng, ưu đãi, nhật ký cuộc gọi và transcript theo kiến trúc Bronze/Silver/Gold. CallCenterEN được chuẩn bị như nguồn dữ liệu call-center bên ngoài để củng cố cơ sở thực nghiệm cho dataset chính, nhưng chưa chốt sẽ được mô hình hóa như stakeholder/dataset song song hay baseline phụ.

## Việc có thể làm trước khi chốt hướng

1. Hoàn thiện profiling CallCenterEN độc lập với `call_code`:
   - số dòng raw, unique, candidate và selected;
   - phân phối domain, inbound/outbound;
   - độ dài transcript, số từ, thời lượng cuộc gọi, ASR confidence;
   - mức độ xuất hiện PII token hoặc PII type nếu có.

2. So sánh CallCenterEN với dataset chính:
   - dataset chính có customer, offer, call_logs, transcript và nhãn `call_code`;
   - CallCenterEN có transcript call-center thật hơn, duration, confidence, domain, direction, PII-redacted token;
   - CallCenterEN không thay thế dataset chính vì không có cùng schema nghiệp vụ và không có ground-truth `call_code`.

3. Chuẩn bị schema Lakehouse trung lập:
   - `bronze_external.callcenteren_raw`: lưu dữ liệu gần nguồn, gồm id, source, transcript, duration, confidence, hash;
   - `silver_external.callcenteren_clean`: chuẩn hóa kiểu dữ liệu, deduplicate theo `text_hash`, thêm domain/direction/length/quality fields;
   - `gold_external.dataset_comparison_metrics`: lưu metric tổng hợp để đưa vào báo cáo hoặc BI.

4. Chuẩn bị kiểm tra pseudo-label khi job 15k hoàn tất:
   - loại label ngoài taxonomy;
   - kiểm tra dòng thiếu label hoặc confidence thấp;
   - lấy mẫu 100-200 dòng để review thủ công;
   - thống kê phân phối label để phát hiện label bị lạm dụng;
   - chỉ gọi label này là pseudo-label/weak label, không gọi là ground truth.

5. Chuẩn bị thí nghiệm mô hình:
   - M0: train bằng dataset chính;
   - M1: train bằng dataset chính + CallCenterEN pseudo-label đã lọc;
   - valid/test vẫn giữ nguyên từ dataset chính để tránh nhiễm đánh giá.

## Trạng thái dữ liệu hiện tại

- Bộ CallCenterEN 15k candidate đã được tạo tại `research_callcenteren_baseline/output/callcenteren_15k_candidate.csv`.
- Summary hiện tại:
  - raw rows: 95.939;
  - unique rows: 90.432;
  - candidate rows: 74.226;
  - selected rows: 15.000;
  - inbound: 11.029;
  - outbound: 3.971;
  - avg duration: 349,5284 giây;
  - median duration: 316 giây;
  - avg confidence: 0,9128;
  - avg chars: 3.746,2027;
  - avg words: 639,8362.

## Việc nên làm ngay sau khi label 15k xong

1. Tạo bảng phân phối `pseudo_call_code`.
2. Lọc các dòng `should_use_for_training = true` và confidence đạt ngưỡng.
3. So sánh số label/dòng, top label, label hiếm và label bất thường.
4. Review thủ công một mẫu nhỏ để ước lượng độ nhiễu.
5. Chỉ sau đó mới quyết định CallCenterEN nên là luồng ngang hàng hay baseline phụ.

