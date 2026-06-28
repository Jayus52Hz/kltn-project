# Thả ảnh capture vào thư mục này

Bạn chụp màn hình hệ thống thật rồi lưu vào thư mục này theo các tên sau:

| File | Nội dung cần chụp |
| --- | --- |
| `cap_01_docker_compose_ps.png` | `docker compose ps -a`, thấy các container chính đang chạy |
| `cap_02_debezium_connector_status.png` | Debezium connector status là `RUNNING` |
| `cap_03_kafka_topics_list.png` | Danh sách Kafka topics của pipeline |
| `cap_04_airflow_dag_graph.png` | Airflow DAG graph của `telesales_lakehouse_pipeline` |
| `cap_05_airflow_success_grid.png` | Airflow latest run success |
| `cap_06_spark_ui_completed_jobs.png` | Spark UI có completed jobs/applications |
| `cap_07_minio_warehouse_buckets.png` | MinIO bucket/warehouse cho Bronze, Silver, Gold |
| `cap_08_superset_end_to_end_dashboard.png` | Superset dashboard end-to-end |
| `cap_09_bigquery_row_counts.png` | BigQuery row count các bảng/view chính |
| `cap_10_bigquery_serving_views.png` | BigQuery Explorer với dataset `kltn0710` và serving views |
| `cap_11_pii_masking_check.png` | Kiểm chứng dữ liệu đã mask/drop PII |
| `cap_12_mongodb_source_collections.png` | MongoDB source collections và document count |

Checklist đầy đủ nằm ở `docs/report_image_assets/IMAGE_CAPTURE_AND_ASSET_CHECKLIST.md`.
