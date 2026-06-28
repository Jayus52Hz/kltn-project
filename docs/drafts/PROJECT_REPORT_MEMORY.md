# Project Report Memory

## Ngữ cảnh tài liệu

- Báo cáo là khóa luận/đồ án tốt nghiệp bằng tiếng Việt, có dấu đầy đủ.
- File Word chính hiện tại: `Report KLTN - 22133056 - Nguyen Quoc Thinh - ch1 ch2 restored.docx`.
- Nội dung đề tài: xây dựng nền tảng dữ liệu Hybrid Data Lakehouse cho hệ thống AGI Telesales.
- Khi viết tiếp, cần dựa trên cả research tài liệu chính thống và repository code trong workspace.

## Rule người dùng đã yêu cầu

- Không sửa trang bìa, lời cảm ơn, phiếu nhận xét, đề cương, kế hoạch và các phần front matter tương tự nếu không được yêu cầu rõ.
- Bắt buộc giữ đúng page break của các trang đầu: trang bìa, lời cảm ơn, đề cương và các trang biểu mẫu phải break đúng như bản gốc.
- Khi format lại báo cáo, toàn bộ bài phải tuân theo một format thống nhất, bám cỡ chữ/font cũ của file gốc.
- Không để lỗi đánh số kiểu `0.1`, `0.2` ở phần mở đầu.
- Heading phải dùng Word heading style thật để tạo/cập nhật mục lục tự động.
- Mỗi chương nên bắt đầu ở trang mới; hết một chương thì page break sang phần/chương tiếp theo để dễ đọc.
- Viết nội dung dài, chi tiết, đúng mức đồ án tốt nghiệp, không viết quá sơ lược.
- Khi cần hình ảnh, để lại note rõ cần ảnh gì để người dùng tự thêm sau.
- Sau khi thêm/sửa nội dung có hình, bảng hoặc viết tắt, phải cập nhật danh mục hình ảnh, danh mục bảng và danh mục ký hiệu/chữ viết tắt.
- Người dùng sẽ review từng phần rồi quyết định viết tiếp thế nào, vì vậy các chương tiếp theo cần làm theo hướng dễ kiểm tra và dễ chỉnh.

## Rule khi viết nội dung

- Văn phong: học thuật, rõ ràng, mạch lạc, phù hợp khóa luận tốt nghiệp ngành kỹ thuật dữ liệu.
- Nội dung research phải ưu tiên tài liệu chính thức: MongoDB, Debezium, Kafka, Spark, Iceberg, Docker, Airflow, MinIO, Superset, scikit-learn và tài liệu pháp lý khi nói về PII.
- Khi viết Chương 2 trở đi, phải bám sát repo code thực tế: `docker-compose.yml`, MongoDB connector, data init, Bronze/Silver/Gold Spark jobs, Airflow DAG, dashboard exporter và README/kết quả kiểm thử.
- Nếu đưa số liệu thực nghiệm, phải lấy từ repo hoặc file dữ liệu/kết quả đã kiểm tra trong workspace.
- Không viết chung chung kiểu marketing; ưu tiên giải thích kiến trúc, luồng dữ liệu, quyết định thiết kế, lý do kỹ thuật và liên hệ trực tiếp với bài toán AGI Telesales.

## Rule khi sửa DOCX

- Trước khi chỉnh DOCX, luôn ưu tiên lấy phần front matter từ file gốc nếu cần rebuild nội dung, để tránh làm hỏng page break và biểu mẫu.
- Không dùng thao tác có nguy cơ phá format toàn cục nếu chỉ cần sửa một phần.
- Sau khi chỉnh, phải kiểm tra:
  - page break phần đầu còn đúng;
  - không xuất hiện heading `0.x`;
  - TOC cập nhật được;
  - danh mục hình/bảng/viết tắt đủ;
  - bảng không tràn, caption đặt đúng, font và spacing nhất quán.
- Với bảng trong báo cáo:
  - bảng viết tắt không cần đánh số trong danh mục bảng;
  - bảng nội dung phải có caption đánh số;
  - caption bảng nên đặt phía trên bảng;
  - bảng dài cần giữ header/lặp header khi sang trang;
  - width, padding, font trong bảng phải được chuẩn hóa để mở trong Word/LibreOffice không bị lệch.
- Sau khi sửa file Word, nếu LibreOffice có sẵn thì render/convert sang PDF để kiểm tra layout thực tế.

## Ghi chú vận hành hiện tại

- LibreOffice đã được cài tại `C:\Program Files\LibreOffice\program\soffice.exe`.
- `soffice` chưa nằm trong PATH, nên khi render cần gọi trực tiếp `C:\Program Files\LibreOffice\program\soffice.com` hoặc thêm tạm thư mục LibreOffice vào PATH trong phiên shell.
- Với đường dẫn workspace có dấu (`D:\Đồ án tốt nghiệp`), nếu LibreOffice/headless lỗi đường dẫn thì copy tạm DOCX sang thư mục ASCII trong `%TEMP%` để render.
## Version notes

### v0 - Baseline before Codex review edits

- Source file used for review: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - revised callcenteren full integrated.docx`.
- Main review artifact: `outputs/review/review_output/academic_review_report.md`.
- Review finding summary:
  - BoW/RoBERTa was inconsistent across body, conclusion, limitation table, and appendix.
  - References existed but in-text citations were missing or inconsistent.
  - TOC/front lists had stale or out-of-order entries.
  - Figures were mostly placeholders; user requested not to insert images yet, only leave notes.

### v1 - Global text, citation, and TOC fixes

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 08-06.docx`.
- Based on the reviewed report and the user's request to fix everything except inserting images.
- Main changes:
  - Standardized model positioning: BoW + Logistic Regression is the production model; RoBERTa is the deep-learning baseline/experimental upgrade path.
  - Fixed stale statements in conclusion, Chapter 4, limitation table, and Appendix D.
  - Fixed wrong wording where RoBERTa was said to be enabled by `NLP_MODEL_TYPE=bow`; correct value is `NLP_MODEL_TYPE=roberta`.
  - Changed "Kết quả dự kiến đạt được" to "Kết quả đạt được".
  - Standardized references into numbered entries `[1]`-`[29]`.
  - Added in-text citations for official technology docs, Lakehouse, scikit-learn/BoW, RoBERTa/BERT/Transformer, CallCenterEN, DAPT, weak supervision, and self-training.
  - Rewrote the static TOC block to remove out-of-order child headings such as `1.14.5`, `2.1.5`, and `4.5.5` appearing before parent sections.
- Verification file: `outputs/review/review_output/text_fixes_verification.txt`.

### v2 - Chapter 1 tool citations and image notes

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 08-06 - chapter1 citations notes.docx`.
- User request: in Chapter 1, each tool should have clear image guidance and clearer references; do not insert actual images yet.
- Main changes:
  - Added clearer citations in Chapter 1 for MongoDB Change Streams, Debezium, Kafka, Spark, Iceberg, MinIO, Airflow, Superset, BigQuery, Looker Studio, and Docker Compose.
  - Added new references:
    - `[30]` MongoDB Change Streams documentation.
    - `[31]` Google Cloud BigQuery overview.
    - `[32]` Looker Studio report tutorial.
    - `[33]` Docker Compose documentation.
  - Added 15 `[NOTE ẢNH - ...]` notes in Chapter 1.
  - Tool-specific image notes were added for:
    - MongoDB Change Streams.
    - Debezium.
    - Apache Kafka.
    - Apache Spark.
    - Apache Iceberg.
    - MinIO/Object Storage.
    - Apache Airflow.
    - Apache Superset.
    - BigQuery/Looker Studio.
    - Docker Compose.
  - Existing conceptual figure notes for Data Warehouse/Data Lake/Lakehouse, Medallion, CDC flow, Iceberg metadata, and PII/NLP flow were preserved.
  - Microsoft Word fields were updated successfully after the DOCX edit.
- Verification file: `outputs/review/review_output/chapter1_citations_notes_verification.txt`.

### v3 - Chapter 1 professional theory expansion

- Current latest file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 08-06 - v3 chuong1 ly thuyet chuyen sau.docx`.
- User request: Chapter 1 theory was still too generic; expand each tool with clearer, more professional, researched theory from the corresponding official web/documentation pages.
- Based on v2 file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 08-06 - chapter1 citations notes.docx`.
- Main changed area: Chapter 1, especially the theory sections for:
  - Data Warehouse/Data Lake/Data Lakehouse distinction.
  - MongoDB Change Streams.
  - Debezium MongoDB connector.
  - Apache Kafka.
  - Apache Spark.
  - Apache Iceberg.
  - MinIO/Object Storage.
  - Apache Airflow.
  - Apache Superset.
  - BigQuery and Looker Studio.
  - Docker Compose and tool-role selection.
- Main content changes:
  - Rewrote the tool theory from generic definitions into mechanism + role in the AGI Telesales architecture + trade-offs/limitations.
  - Clarified why the pipeline separates OLTP, CDC transport, compute, Lakehouse table/storage, orchestration, and BI layers.
  - Kept the existing citation numbers `[4]`-`[11]` and `[30]`-`[33]`, and strengthened how they are used in Chapter 1.
  - Preserved the 15 image notes from v2; actual image insertion is still unresolved/manual as requested.
  - Updated Microsoft Word fields successfully after the DOCX edit.
- Verification file: `outputs/review/review_output/v3_chapter1_theory_verification.txt`.
- QA result:
  - Structural/content verification passed for all expanded tool sections.
  - Chapter 1 still has 15 `[NOTE ANH - ...]` placeholders.
  - Visual render QA was attempted but could not run because `C:\Program Files\LibreOffice\program` exists without a `soffice` executable in the current environment.

### Versioning rule for future edits

- Treat each accepted DOCX edit batch as a new version.
- Next edit batch should be recorded as `v3`, then `v4`, and so on.
- For each version note, record:
  - output DOCX filename;
  - user request summary;
  - files/chapters changed;
  - major content changes;
  - verification file or QA result;
  - unresolved manual work, especially image insertion.

### v4 - CallCenterEN multi-source Lakehouse integration

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 20-06 - v4 callcenteren multisource lakehouse.docx`.
- User request: bổ sung vào báo cáo phần CallCenterEN vừa triển khai, dùng văn phong học thuật và bám evidence từ repo.
- Based on v3 file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 08-06 - v3 chuong1 ly thuyet chuyen sau.docx`.
- Main changed areas:
  - Chapter 2: updated CallCenterEN role from external-only baseline to second first-class dataset branch in a multi-source Hybrid Data Lakehouse; updated pseudo-label and split numbers.
  - Chapter 2: replaced old auxiliary-training setup with separate CallCenterEN model positioning.
  - Chapter 4: replaced old 300-row auxiliary experiment narrative with verified fine-tune metrics, 15k model_call_code application, and Lakehouse write evidence.
  - Chapter 5: updated contribution, limitation, and conclusion sections to reflect domain shift, separate models, weak-label limitations, and local Lakehouse completion.
- Main verified figures:
  - CallCenterEN candidate dataset: 15,000 rows.
  - Split after quality filter: 2,260 rows = 1,598 train, 315 validation, 347 test.
  - Chosen CallCenterEN model: `count_word_lr_threshold`.
  - Test metrics: exact match 0.1412, avg Jaccard 0.5943, micro-F1 0.7241, macro-F1 0.1933, weighted-F1 0.7128.
  - Lakehouse tables written: 15,000 rows in bronze_external/silver_external CallCenterEN tables and Gold comparison tables created.
- Verification file: `outputs/review/review_output/v4_callcenteren_multisource_verification.txt`.
- QA result:
  - DOCX opens successfully with python-docx.
  - Heading count: 201; empty heading count: 0; table count: 71; zero-dimension table count: 0.
  - Stale phrases `300 pseudo-label`, `auxiliary training với CallCenterEN`, and `external baseline và auxiliary corpus` no longer appear.
  - Visual render QA could not be completed because `soffice`/LibreOffice is not available in the current environment.

### v4 cleanup - Academic wording and table-list fixes

- Output file updated in place: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 20-06 - v4 callcenteren multisource lakehouse.docx`.
- User request: sửa theo review học thuật, loại bỏ các phần mang tính lời nhắc/hướng dẫn và viết lại theo hướng học thuật, chuyên nghiệp.
- Main changes:
  - Removed instruction-like wording such as "khi chụp", "khi nộp", "báo cáo nên", and fragment-style notes.
  - Converted `[NOTE ẢNH - ...]` placeholders to neutral `[MINH CHỨNG HÌNH - ...]` placeholders.
  - Updated opening section to mention CallCenterEN as a multi-source Lakehouse contribution.
  - Updated static table list with CallCenterEN tables.
  - Renumbered CallCenterEN experiment tables to `Bảng 4.5`, `Bảng 4.6`, and `Bảng 4.7`.
  - Added a source/protocol note before the two-model comparison table.
  - Rewrote section `4.6.2. Các lỗi thực nghiệm đã ghi nhận` in academic prose.
- Verification file: `outputs/review/review_output/v4_academic_cleanup_verification.txt`.
- QA result:
  - DOCX opens successfully with python-docx.
  - Paragraph count: 1,089; table count: 72; empty heading count: 0; zero-dimension table count: 0.
  - Instructional/stale term checks passed for `[NOTE]`, `Chèn`, `khi chụp`, `khi nộp`, `báo cáo nên`, `cần chụp`, `mình sẽ`, `tôi sẽ`, `phần này hướng dẫn`, `thiếu dependency cho`, and `Bảng 4.9`.
  - Visual render QA still could not be completed because `soffice`/LibreOffice is not available in the current environment.

### v5 - Airflow staged pipeline verification

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 20-06 - v5 airflow staged pipeline test.docx`.
- User request: test toàn bộ luồng sau khi tách Airflow theo stage/dataset, review kết quả và viết lại vào báo cáo.
- Based on v4 file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 20-06 - v4 callcenteren multisource lakehouse.docx`.
- Main changed area:
  - Chapter 4: added section `4.6.3. Kiểm thử luồng Airflow tách stage theo dataset`.
- Main verified results:
  - Static Python compile passed for Airflow DAG and Spark jobs.
  - Docker Compose stack started successfully after restarting Kafka/Debezium services; Kafka initially failed because Zookeeper still held a stale broker ephemeral node `/brokers/ids/1`.
  - Airflow DAG `telesales_lakehouse_pipeline` parsed successfully and exposed the staged task graph.
  - Manual run `manual_stage_test_20260620_200514` completed with state `success`.
  - Run time: 2026-06-20T13:05:16+00:00 to 2026-06-20T13:10:40.473161+00:00, about 5 minutes 24 seconds.
  - Primary Lakehouse counts after dedup: `silver.cust = 4,344`, `silver.offer = 5,072`, `silver.call_logs = 23,447`, `gold.fact_telesales_calls = 23,447`.
  - CallCenterEN branch counts: `bronze_external.callcenteren_raw = 15,000`, `silver_external.callcenteren_clean = 15,000`, `silver_external.callcenteren_labeled = 15,000`.
  - BigQuery counts matched Lakehouse counts; `vw_telesales_performance = 23,447`.
  - Static dashboard export succeeded and produced `project/dashboard/dashboard_data.json` with 8 KPI groups, 6 chart groups, and 2 table groups.
- Verification files:
  - `outputs/review/review_output/v5_stage_airflow_pipeline_test_verification.md`.
  - `outputs/review/review_output/v5_stage_airflow_report_section.txt`.
- QA result:
  - DOCX opens successfully with python-docx.
  - Paragraph count: 1,098; table count: 72; heading count: 202.
  - New heading `4.6.3. Kiểm thử luồng Airflow tách stage theo dataset` uses `Heading 3`.
  - Empty heading count: 0; `0.x` heading count: 0.
  - Visual render QA could not be completed because `soffice`/LibreOffice is not available in the current environment.

### v6 - Code/report consistency review

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 24-06 - v6 code report consistency.docx`.
- User request: rerun Docker where possible, review the full code against report logic, and update the report if needed.
- Based on v5 file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 20-06 - v5 airflow staged pipeline test.docx`.
- Main changed areas:
  - Updated stale Airflow descriptions from the old single-chain task names (`bronze_cdc_ingestion`, `silver_etl`, `gold_star_schema`) to the current staged DAG with per-dataset Bronze/Silver tasks, Gold dimension/fact tasks, CallCenterEN external tasks, and `bq_sync_gold`.
  - Updated CallCenterEN data preparation tables from the older 3,000/2,000/300 auxiliary framing to the current 15,000 candidate dataset, 2,420 pseudo-label rows, and 2,260 train/valid/test split.
  - Updated experiment tables to reflect current `M0_primary_bow` and `M4_combined_bow` metrics from `research_callcenteren_baseline/output/multisource_bow/multisource_bow_experiment_metrics.csv`.
  - Updated Silver/Gold/BigQuery wording so the report describes `call_code` as the model-generated downstream label and treats `call_code_original`/`call_code_predicted` as legacy/intermediate columns only.
  - Updated BigQuery publish security wording from the old `BQ_INCLUDE_PII=false` flag to the current `BLOCKED_BIGQUERY_COLUMNS` implementation in `project/batch-etl/bq_sync_job.py`.
- Docker verification result on 2026-06-24:
  - `docker compose -f .\project\docker-compose.yml up -d` started the stack after Kafka initially failed on a stale Zookeeper ephemeral broker node.
  - Kafka, Debezium Connect, MinIO, Spark, Airflow, Superset, MongoDB, and Postgres reached running/healthy states.
  - `mongo-data-init` loaded/updated `cust=4,344`, `offer=5,072`, and `call_logs=23,447`.
  - Debezium connector `mongo-source` returned `connector=RUNNING` and task `RUNNING`.
  - Manual Airflow run `manual_report_review_20260624_103221` was triggered, but the observed task state stopped at `wait_for_debezium_connector=up_for_retry`; shortly after, Docker Desktop server API started returning HTTP 500 on `/version` and container inspection routes, so the new run could not be followed to a terminal state in this session.
  - A follow-up attempt after restarting Docker Desktop and bringing the stack up again reached `debezium_connect=Healthy`, but Docker Desktop API again returned HTTP 500 for `compose ps`, container logs, and `docker exec`; no reliable Airflow terminal state could be collected.
  - v5's successful run `manual_stage_test_20260620_200514` remains the latest complete end-to-end evidence.
- QA result:
  - DOCX opens successfully with python-docx.
  - Paragraph count: 1,098; table count: 72.
  - Stale keyword checks passed for `bronze_cdc_ingestion`, `silver_etl`, `gold_star_schema`, `BQ_INCLUDE_PII`, and accidental `call_code_original/call_code`.
  - `call_code_predicted` remains only in the literal BigQuery blocklist example, matching the current code's legacy-column drop behavior.
  - Visual render QA could not be completed because `soffice`/LibreOffice is not available in the current environment.

### v7 - CallCenterEN Star Schema and BigQuery verification

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 26-06 - v7 callcenteren star schema bq verification.docx`.
- User request: test the full existing AGI Telesales flow and the CallCenterEN flow through BigQuery, fix remaining errors, keep CallCenterEN on the same Bronze/Silver/Gold pattern with BoW inference in Silver, schedule the dataset flows sequentially, and update the report.
- Based on v6 file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 24-06 - v6 code report consistency.docx`.
- Main code changes verified:
  - `callcenteren_external_job.py` now loads the CallCenterEN BoW model in Silver, infers `call_code`, writes `silver_external.callcenteren_labeled`, and creates CallCenterEN Star Schema tables in Gold.
  - `telesales_pipeline.py` schedules primary dataset stages before CallCenterEN and runs `bq_sync_gold` last.
  - `bq_sync_job.py` syncs CallCenterEN Star Schema, comparison tables, analytics table, and sanitized `callcenteren_labeled`.
  - `create_serving_views.sql` creates primary and CallCenterEN serving views with sandbox-compatible expiration.
- Full Airflow verification:
  - Final clean run id: `manual_clean_v7_20260626_091000`.
  - Runtime: `2026-06-26T02:09:33.435820+00:00` to `2026-06-26T02:24:13.937154+00:00`.
  - Primary Bronze/Silver/Gold tasks succeeded.
  - CallCenterEN `bronze`, `silver`, and `gold` succeeded.
  - `bq_sync_gold` succeeded after BigQuery sandbox TTL was configured and the sandbox sync path was switched to the DataFrame load API to avoid transient BigQuery Storage Write API stream `NOT_FOUND` failures.
  - Earlier debug run `manual_full_bq_callcenter_20260626_011230` was used during fixes but later shows `failed` because `bq_sync_gold` was cleared during manual BigQuery restoration; do not use that as the final v7 evidence run.
  - Intermediate scheduled run `scheduled__2026-06-25T02:00:00+00:00` also completed successfully before the final manual clean run.
- Lakehouse counts:
  - Primary Gold: `dim_customer=4,344`, `dim_offer=5,072`, `dim_date=2`, `fact_telesales_calls=23,447`.
  - CallCenterEN Silver: `callcenteren_clean=15,000`, `callcenteren_labeled=15,000`.
  - CallCenterEN Star Schema: `dim_callcenter_source=9`, `dim_callcenter_model=1`, `dim_call_code=28`, `fact_callcenter_calls=15,000`, `bridge_callcenter_call_code=49,891`.
  - Comparison/analytics: `callcenteren_call_analytics=8`, `dataset_profile_comparison=2`, `call_code_distribution_comparison=59`, `model_experiment_comparison=6`.
- BigQuery verification:
  - Dataset `project-ef0c6db5-0765-4391-845.kltn0710` is in sandbox mode, so `default_table_expiration` and `default_partition_expiration` were set to `5,097,600` seconds.
  - BigQuery table counts matched Lakehouse counts.
  - Serving views verified: `vw_telesales_performance=23,447`, `vw_callcenteren_labeled=15,000`, `vw_callcenteren_performance=15,000`, `vw_callcenteren_call_codes=49,891`.
- Report changes:
  - Added section `4.6.4. Kiểm thử full pipeline tuần tự ngày 26/06/2026`.
  - Added `Bảng 4.8. Kết quả kiểm thử full pipeline tuần tự ngày 26/06/2026`.
  - Updated BigQuery sync wording, CallCenterEN serving limitations, conclusion, and v7 version note.
- Verification file: `outputs/review/review_output/v7_callcenteren_star_schema_bq_verification.md`.
- QA result:
  - `python -m py_compile project/batch-etl/callcenteren_external_job.py project/batch-etl/bq_sync_job.py project/airflow/dags/telesales_pipeline.py` passed.
  - DOCX opens with python-docx.
  - Paragraph count: 1,108; table count: 73; required markers present; exactly one v7 verification table detected.
  - Visual render QA could not be completed because `soffice`/LibreOffice is not available in the current Windows environment.

### v8 - Equal multi-source framing and model comparison

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v8 multisource equal branches model comparison.docx`.
- Based on v7 file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 26-06 - v7 callcenteren star schema bq verification.docx`.
- User request: revise the report so CallCenterEN is treated as an equivalent data branch, not as an add-on after the original dataset, and add a post-experiment model comparison table to justify model selection.
- Main report changes:
  - Reframed AGI Telesales and CallCenterEN as two equivalent branches in the final multi-source Lakehouse architecture.
  - Renamed section `2.1.5` to `Quan he thiet ke giua hai nhanh du lieu` and section `2.3.5` to `So sanh hai nhanh du lieu AGI Telesales va CallCenterEN`.
  - Rewrote the CallCenterEN data-role discussion to separate historical prompt-design influence from final equal-branch architecture.
  - Added `Bang 4.5. So sanh hau thuc nghiem giua BoW va RoBERTa de chon mo hinh production`.
  - Renumbered downstream Chapter 4 tables to `Bang 4.6` through `Bang 4.9`.
  - Fixed the DAG wording so sequential scheduling is described as local resource orchestration, not a main/sub dataset relationship.
  - Fixed the mojibake BigQuery confirmation sentence in section `4.6.4`.
- QA result:
  - DOCX opens with python-docx.
  - Paragraph count: 1,111; table count: 74.
  - Front table list contains `Bang 4.1` through `Bang 4.9`.
  - New model-comparison table is table 65 with 6 rows and 4 columns.
  - Stale phrasing checks returned zero hits for `dataset cu`, `phan mo rong CallCenterEN`, `bo sung CallCenterEN`, `corpus phu tro`, `prompt sinh dataset chinh`, and mojibake `l?n ch?y`.
  - Visual render QA could not be completed because `soffice`/LibreOffice is not available, and Microsoft Word COM export hung in the current Windows environment and was stopped.

### v9 - Academic review cleanup

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v9 academic review cleanup.docx`.
- Based on v8 file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v8 multisource equal branches model comparison.docx`.
- User request: use `academic-report-writer` to review the report.
- Main report changes:
  - Fixed academic wording that still read like instructions, including phrases such as `bao cao can`, `bao cao nen`, `can chup`, and screenshot guidance inside the prose.
  - Fixed the broken heading `4.5.5. Thuc nghiem CallCenterEN nhu nhanh nhanh AGI Telesales`.
  - Updated the version note from v7/v8 wording to `Ghi chu phien ban v9`.
  - Renumbered figure captions by the actual chapter context and rebuilt `DANH MUC HINH ANH` from body captions.
  - Renumbered table captions by the actual chapter context and rebuilt `DANH MUC BANG` from body captions.
  - Kept the equal-branch framing for AGI Telesales and CallCenterEN, and kept the post-experiment BoW/RoBERTa comparison table.
- QA result:
  - DOCX opens with python-docx.
  - Paragraph count: 1,105; table count: 74.
  - Figure list matches all 22 body figure captions.
  - Table list matches all 25 body table captions.
  - Stale/meta phrasing checks returned zero hits for `can chup`, `bao cao can`, `bao cao nen`, `placeholder`, `Ghi chu phien ban v7`, `nhanh nhanh`, and stale `Bang 4.5 tong hop`.
  - Visual render QA still could not be completed because `soffice`/LibreOffice is not available in the current Windows environment.

### v10 - Abstracts, commitment, and expanded related work

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v10 abstracts commitment related-work.docx`.
- Based on v9 file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v9 academic review cleanup.docx`.
- User request: add missing Vietnamese summary, English abstract, commitment/declaration, and expand section `1.4. Mot so cong trinh nghien cuu...` into stronger related work/gap analysis.
- Main report changes:
  - Added `Loi cam doan` after `Loi cam on` in the front matter.
  - Added Vietnamese `Tom tat` covering problem, method, architecture, two equivalent dataset branches, NLP model choice, verification results, and limitations.
  - Added English `Abstract` with matching scope and results.
  - Rewrote section `1.4` from 3 short paragraphs to 9 ordered paragraphs covering Lakehouse/Iceberg, CDC/Debezium/MongoDB Change Streams, CallCenterEN/NLP/weak labels/domain shift, BI serving, and the final research gap.
  - Updated version note to `Ghi chu phien ban v10`.
- QA result:
  - DOCX opens with python-docx.
  - Paragraph count: 1,130; table count: 74.
  - Required front matter markers present: `Loi cam doan`, `Tom tat`, `Abstract`.
  - Section `1.4` body paragraph count: 9.
  - Figure list matches all 22 body figure captions.
  - Table list matches all 25 body table captions.
  - Stale/meta phrasing checks returned zero hits for `can chup`, `bao cao can`, `placeholder`, `Phien ban v9`, and stale CallCenterEN-as-subordinate wording.
  - Visual render QA still could not be completed because `soffice`/LibreOffice is not available in the current Windows environment.

### v11 - Format standardization

- Output file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v11 format standardization.docx`.
- Based on v10 file: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v10 abstracts commitment related-work.docx`.
- User request: standardize format, font, margins, and report-format compliance.
- Main report changes:
  - Set A4 page setup and margins: top `3.0 cm`, bottom `3.5 cm`, left `3.5 cm`, right `2.0 cm`, header/footer distance `2.0 cm`.
  - Set `Normal` style to Times New Roman, `13 pt`, justify, line spacing `1.2`.
  - Standardized heading and caption styles to Times New Roman with line spacing `1.2`.
  - Added Word `PAGE` field in footer and enabled `w:updateFields=true`.
  - Converted all real body figure/table captions to `Report Caption`.
  - Rebuilt `DANH MUC HINH ANH` and `DANH MUC BANG` from all body captions.
  - Renumbered body captions by actual chapter context.
  - Fixed Chapter 1 table numbering so it runs continuously from `Bang 1.1` to `Bang 1.13`.
- QA result:
  - DOCX opens with python-docx.
  - Paragraph count: 1,148; table count: 74.
  - Figure list matches all 27 body figure captions.
  - Table list matches all 38 body table captions.
  - All real body captions use `Report Caption`.
  - Footer contains a `PAGE` field.
  - Visual render QA still could not be completed because `soffice`/LibreOffice is not available in the current Windows environment.
  - Static TOC remains text-only and page numbers still require Word/LibreOffice field/layout update.
