# Phân tích CallCenterEN để thiết kế prompt và dataset chính

## Mục đích

CallCenterEN được sử dụng như bước nghiên cứu nền trước khi mô tả và bảo vệ dataset chính. Mạch phương pháp là: phân tích transcript call center thực tế -> trích xuất đặc trưng nghiệp vụ/ngôn ngữ -> chuyển thành nguyên tắc prompt -> sinh dataset telesales chính có schema và nhãn phù hợp.

## Thống kê từ 3,000 mẫu baseline

- Số mẫu phân tích: 3,000
- Độ dài trung bình: 3,890.2737 ký tự, 644.6783 từ
- Thời lượng trung bình: 348.4023 giây
- ASR confidence trung bình: 0.9534
- PII token trung bình: 58.8953

### Phân bố domain

| Domain | Số mẫu |
|---|---:|
| `insurance` | 2,694 |
| `customer_service` | 306 |

### Phân bố hướng cuộc gọi

| Hướng | Số mẫu |
|---|---:|
| `outbound` | 1,666 |
| `inbound` | 1,334 |

### Đặc trưng hội thoại phát hiện bằng keyword groups

| Nhóm đặc trưng | Số mẫu | Tỷ lệ |
|---|---:|---:|
| `opening` | 2,999 | 99.97% |
| `product_or_offer` | 2,959 | 98.63% |
| `identity_verification` | 2,935 | 97.83% |
| `fee_discussion` | 2,560 | 85.33% |
| `needs_analysis` | 2,452 | 81.73% |
| `handoff_or_followup` | 2,181 | 72.7% |
| `objection_or_rejection` | 537 | 17.9% |

### Mapping từ CallCenterEN sang prompt dataset chính

| Quan sát từ CallCenterEN | Quy tắc prompt cho dataset chính | Trường/nhãn trong dataset chính |
|---|---|---|
| Transcript có lời chào, IVR hoặc câu mở đầu của agent/customer. | Bắt buộc transcript sinh ra có đoạn OPENING rõ ràng, có speaker Agent/Customer. | `call_transcript, OPENING` |
| Nhiều cuộc gọi có xác minh thông tin như tên, tuổi, địa chỉ, số điện thoại, ngày sinh. | Sinh customer profile và đưa một phần thông tin vào hội thoại, sau đó pipeline phải xử lý PII. | `customer fields, PII masking` |
| Agent thường hỏi nhu cầu, tình trạng hiện tại, coverage, household hoặc khả năng chi trả. | Prompt phải yêu cầu đoạn NEEDS_ANALYSIS trước khi pitch sản phẩm. | `NEEDS_ANALYSIS` |
| Các cuộc gọi bảo hiểm/customer service có thảo luận quote, premium, payment, plan, benefit. | Prompt sinh offer/product và yêu cầu transcript chứa PRODUCT_PITCH/FEE_DISCUSSION khi phù hợp. | `offer, product_name, PRODUCT_PITCH, FEE_DISCUSSION` |
| Khách hàng có thể đồng ý thụ động, phản đối, từ chối, yêu cầu gọi lại hoặc kết thúc đột ngột. | Prompt phải tạo outcome đa dạng: WARM_LEAD, SOFT_REJECTION, HARD_REJECTION, DO_NOT_CALL_REQUEST, SUDDEN_HANG_UP. | `call_code, outcome flags` |
| Dữ liệu thực tế có inbound/outbound và nhiều domain. | Dataset chính giữ domain telesales tài chính nhưng vẫn mô phỏng các kịch bản lead source, campaign và sản phẩm khác nhau. | `lead_source, campaign_id, product_name` |
| Có metadata về audio_duration và confidence. | Dataset chính cần có talk_time_seconds; ASR confidence được ghi là hướng mở rộng nếu có audio/ASR thật. | `talk_time_seconds` |

## Kết luận phương pháp

Phần 3,000 mẫu CallCenterEN không phải tập training pseudo-label. Nó là tập phân tích đặc trưng để chứng minh dataset chính không được sinh tùy tiện. Từ các đặc trưng như opening, xác minh thông tin, needs analysis, pitch, fee discussion, objection/rejection, PII và duration, prompt sinh dataset chính được thiết kế để tạo transcript có cấu trúc nghiệp vụ và có nhãn call_code.

Phần 300 pseudo-label là một thí nghiệm riêng, nhỏ hơn, dùng để kiểm tra việc CallCenterEN có thể tham gia training như auxiliary corpus. Do đó cần phân biệt rõ: 3,000 mẫu dùng cho phân tích thiết kế dataset/prompt; 300 mẫu dùng cho pilot weak-label training.