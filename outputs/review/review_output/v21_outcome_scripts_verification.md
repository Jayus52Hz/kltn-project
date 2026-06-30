# v21 outcome scripts verification

## Output

- File: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 30-06 - v21 outcome scripts.docx`
- Based on: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 30-06 - v20 conclusion compact.docx`

## Nội dung đã bổ sung

- Thêm mục `3.5.4. Sinh kịch bản telesales theo outcome_strategy`.
- Thêm mục `4.6.5. Kiểm thử sinh kịch bản theo outcome_strategy ngày 30/06/2026`.
- Thêm `Bảng 4.9. Kết quả kiểm thử sinh kịch bản theo outcome_strategy ngày 30/06/2026`.
- Cập nhật `DANH MỤC BẢNG` với `Bảng 4.9`.
- Cập nhật phần kết luận về kết quả, đóng góp, hạn chế và hướng phát triển liên quan đến bảng `customer_outcome_scripts`.
- Bổ sung Phụ lục D với các artifact mới: `outcome_script_job.py`, `outcome_script_rules.py`, `create_serving_views.sql`.

## Evidence được đưa vào báo cáo

- Spark job `outcome_script_job.py` merge thành công `lakehouse.gold.customer_outcome_scripts` với `23.447` dòng.
- Phân bố outcome đã kiểm chứng:
  - `CALLBACK`: `5.248`
  - `DO_NOT_CALL`: `4.503`
  - `HARD_REJECTION`: `2.268`
  - `IN_PROGRESS`: `5.137`
  - `SALE`: `3.933`
  - `SOFT_REJECTION`: `2.358`
- BigQuery sync ghi bảng `project-ef0c6db5-0765-4391-845.kltn0710.customer_outcome_scripts` với `23.447` dòng.
- BigQuery view `vw_customer_outcome_scripts` trả `23.447` dòng.
- Mẫu `DO_NOT_CALL` không pitch tiếp sản phẩm và dùng `next_action = ADD_TO_DO_NOT_CALL_LIST`.
- Airflow DAG không có import error; task `customer_outcome_scripts` nằm sau `fact_telesales_calls` và trước `bq_sync_gold`.

## Structural QA

- DOCX mở được bằng `python-docx`.
- Paragraph count: `992`.
- Table count: `75`.
- Heading count: `190`.
- Report caption count: `74`.
- Required markers present:
  - `3.5.4`
  - `4.6.5`
  - `Bảng 4.9`
  - `customer_outcome_scripts`
  - `vw_customer_outcome_scripts`
- `Bảng 4.9` là table index `73`, gồm `7` rows và `3` columns.
- Phụ lục D là table cuối, gồm `10` rows và `4` columns.
- `0.x` heading count: `0`.
- Stale `Chương 5` count: `0`.

## Render QA

- Visual render QA chưa thực hiện được vì `soffice`/LibreOffice không có trong môi trường hiện tại:
  - `C:\Program Files\LibreOffice\program\soffice.com`: not found.
  - `C:\Program Files\LibreOffice\program\soffice.exe`: not found.
- Packaged `render_docx.py` không chạy được do không tìm thấy executable `soffice`.
