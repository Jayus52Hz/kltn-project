# Checklist ảnh còn thiếu cho báo cáo KLTN

Thư mục dùng chung:

- Ảnh đã generate sẵn: `docs/report_image_assets/generated/`
- Ảnh bạn tự capture và thả vào: `docs/report_image_assets/capture_here/`
- Script generate lại ảnh: `scripts/reporting/generate_report_image_assets.py`

## 1. Ảnh đã generate sẵn

Các ảnh dưới đây là hình minh họa tự tạo, dùng được trực tiếp trong báo cáo. Không phụ thuộc đường dẫn web, không cần download lại.

| File | Dùng cho phần nào trong báo cáo | Vai trò |
| --- | --- | --- |
| `generated/fig_01_data_warehouse_lake_lakehouse.png` | Hình 1.1 / cơ sở lý thuyết Data Warehouse, Data Lake, Lakehouse | Giải thích tiến hóa kiến trúc dữ liệu |
| `generated/fig_02_medallion_overview.png` | Hình 1.2 / Bronze, Silver, Gold | Mô tả cách dữ liệu đi qua các tầng lakehouse |
| `generated/fig_03_cdc_mongodb_debezium_kafka_bronze.png` | CDC MongoDB, Debezium, Kafka, Bronze | Giải thích nhánh ingest streaming/CDC |
| `generated/fig_04_iceberg_metadata_snapshot_manifest.png` | Apache Iceberg / Hình 1.4 | Minh họa metadata, snapshot, manifest và data files |
| `generated/fig_05_silver_pii_nlp_flow.png` | Silver layer / Hình 1.5 | Mô tả chuẩn hóa, masking PII và NLP enrichment |
| `generated/fig_06_synthetic_data_generation.png` | Quy trình sinh dữ liệu tổng hợp bằng Google Generative AI | Giải thích nguồn dữ liệu giả lập cho đồ án |
| `generated/fig_07_master_data_schema.png` | Schema master data trước chuẩn hóa | Minh họa cấu trúc bản ghi telesales ban đầu |
| `generated/fig_08_call_code_distribution.png` | Phân bố nhãn `call_code` | Thể hiện phân bố nhãn từ dữ liệu dashboard |
| `generated/fig_09_json_normalization_entities.png` | Luồng chuẩn hóa JSON thành customers, offers, calls | Giải thích tách entity từ raw document |
| `generated/fig_10_overall_hybrid_lakehouse_architecture.png` | Kiến trúc tổng thể hệ thống | Hình chính để trình bày toàn bộ pipeline |
| `generated/fig_11_data_lineage_mongodb_bigquery.png` | Data lineage MongoDB đến BigQuery | Chứng minh đường đi dữ liệu end-to-end |
| `generated/fig_12_sequence_cdc_batch_etl.png` | Sequence diagram CDC và batch ETL | Giải thích thứ tự tương tác giữa service |
| `generated/fig_13_medallion_layer_responsibilities.png` | Medallion Architecture | Nêu trách nhiệm từng tầng Bronze/Silver/Gold |
| `generated/fig_14_gold_star_schema.png` | Star Schema lớp Gold | Minh họa fact/dimension phục vụ BI |
| `generated/fig_15_pii_control_flow.png` | Luồng kiểm soát PII | Giải thích PII được mask/drop trước serving |
| `generated/fig_16_docker_compose_container_map.png` | Bản đồ container Docker Compose | Minh họa các service trong môi trường local |
| `generated/fig_17_gold_to_bigquery_serving.png` | Gold Iceberg sang BigQuery serving view | Giải thích bước sync dữ liệu cho BI |
| `generated/fig_18_production_roadmap.png` | Roadmap nâng cấp production | Dùng cho phần kết luận/hướng phát triển |

## 2. Ảnh bạn cần tự capture

Các ảnh này nên chụp từ hệ thống thật để làm bằng chứng thực nghiệm. Bạn chụp xong đặt đúng tên file và bỏ vào `docs/report_image_assets/capture_here/`.

| Tên file nên lưu | Chụp ở đâu | Cần thấy gì trong ảnh | Dùng để chứng minh |
| --- | --- | --- | --- |
| `cap_01_docker_compose_ps.png` | Terminal | Kết quả `docker compose -f ".\project\docker-compose.yml" ps -a` | Toàn bộ container chính đang chạy |
| `cap_02_debezium_connector_status.png` | Terminal hoặc browser | Status connector MongoDB Debezium là `RUNNING` | CDC connector hoạt động |
| `cap_03_kafka_topics_list.png` | Terminal | Danh sách Kafka topics cho `cust`, `offer`, `call_logs` hoặc topic CDC tương ứng | Kafka nhận dữ liệu từ CDC |
| `cap_04_airflow_dag_graph.png` | Airflow UI `http://localhost:8081` | DAG `telesales_lakehouse_pipeline` ở Graph view, thấy các nhóm Bronze/Silver/Gold/BigQuery | Airflow điều phối pipeline |
| `cap_05_airflow_success_grid.png` | Airflow UI | Grid view/latest run màu xanh success | Pipeline chạy thành công end-to-end |
| `cap_06_spark_ui_completed_jobs.png` | Spark UI `http://localhost:8080` | Spark applications/jobs hoàn tất cho các bước ETL | Spark thực thi xử lý dữ liệu |
| `cap_07_minio_warehouse_buckets.png` | MinIO Console `http://localhost:9001` | Bucket/warehouse chứa dữ liệu Bronze, Silver, Gold hoặc Iceberg warehouse | Object storage lưu dữ liệu lakehouse |
| `cap_08_superset_end_to_end_dashboard.png` | Superset `http://localhost:8088/superset/dashboard/2/` | Dashboard `KLTN Hybrid Lakehouse - End-to-End BI Dashboard`, thấy KPI và charts | BI dashboard đọc dữ liệu serving |
| `cap_09_bigquery_row_counts.png` | BigQuery Console | Query row count cho các view/table chính | BigQuery có dữ liệu sau sync |
| `cap_10_bigquery_serving_views.png` | BigQuery Explorer | Dataset `kltn0710` và các view `vw_telesales_performance`, `vw_callcenteren_performance`, `vw_callcenteren_call_codes` | Serving layer đã sẵn sàng cho BI |
| `cap_11_pii_masking_check.png` | Terminal/Spark SQL/BigQuery | Dữ liệu đã mask/drop PII, không lộ phone/id/transcript trực tiếp ở serving | Đáp ứng yêu cầu bảo vệ dữ liệu cá nhân |
| `cap_12_mongodb_source_collections.png` | MongoDB Compass hoặc `mongosh` | Collections nguồn và row/document count | Chứng minh dữ liệu đầu vào |

## 3. Ảnh tùy chọn

| Tên file nên lưu | Khi nào cần | Ghi chú |
| --- | --- | --- |
| `cap_13_airflow_task_logs.png` | Nếu cần bằng chứng chi tiết hơn | Chụp log task Bronze/Silver/Gold có trạng thái success |
| `cap_14_superset_chart_detail.png` | Nếu hội đồng hỏi chart lấy từ dataset nào | Chụp chart detail hoặc Explore view trong Superset |
| `cap_15_looker_studio_dashboard.png` | Chỉ khi bạn thật sự có Looker Studio | Nếu chưa triển khai, không cần chụp; báo cáo nên mô tả Looker Studio là hướng mở rộng |

## 4. Ưu tiên khi đưa vào báo cáo

Nên đưa vào báo cáo tối thiểu các ảnh sau:

1. `fig_10_overall_hybrid_lakehouse_architecture.png`
2. `fig_03_cdc_mongodb_debezium_kafka_bronze.png`
3. `fig_13_medallion_layer_responsibilities.png`
4. `fig_14_gold_star_schema.png`
5. `fig_17_gold_to_bigquery_serving.png`
6. `cap_04_airflow_dag_graph.png`
7. `cap_05_airflow_success_grid.png`
8. `cap_08_superset_end_to_end_dashboard.png`
9. `cap_09_bigquery_row_counts.png`
10. `cap_11_pii_masking_check.png`

## 5. Ghi chú khi capture

- Chụp màn hình rõ chữ, ưu tiên full browser/terminal thay vì crop quá sát.
- Nếu có thông tin nhạy cảm thật, che trước khi bỏ vào báo cáo.
- Mỗi ảnh nên có caption trong báo cáo theo mẫu: "Hình X.Y. Kiểm chứng ...".
- Không cần dùng ảnh logo download từ Internet; bộ ảnh generated đã đủ cho phần minh họa kiến trúc và tránh lỗi bản quyền/đường dẫn.
