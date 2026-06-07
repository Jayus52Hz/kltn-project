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
