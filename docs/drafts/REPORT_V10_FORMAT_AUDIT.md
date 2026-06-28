# Format audit cho bao cao v10

File audit: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v10 abstracts commitment related-work.docx`

Ngay audit: 27/06/2026

Nguon doi chieu chinh: HCMUTE FAS, `Huong dan trinh bay khoa luan tot nghiep 2019`, `https://fas.hcmute.edu.vn/Resources/Docs/SubDomain/fas/Thong%20bao/Final_Huong%20dan%20trinh%20bay%20khoa%20luan%20tot%20nghiep%202019.pdf`.

## Ket luan ngan

Ban v10 **chua dat chuan format neu doi chieu nghiem voi guideline HCMUTE 2019**. Noi dung va cau truc khoa luan da day du hon, nhung format Word can sua truoc khi nop: le trang, co chu body, gian dong, so trang, danh muc hinh/bang va style caption.

## Bang doi chieu chinh

| Tieu chi | Chuan doi chieu | Hien trang v10 | Ket luan |
| --- | --- | --- | --- |
| Kho giay | A4 | 21.01 x 29.69 cm | Dat |
| Le trang | Tren 3.0 cm, duoi 3.5 cm, trai 3.5 cm, phai 2.0 cm | Tren/duoi/trai/phai deu 2.03 cm | Khong dat |
| Header/footer | Khoang 2.0 cm | Header 0.99 cm, footer 1.27 cm | Khong dat |
| Font body | Times New Roman, Unicode | Normal style Times New Roman | Gan dat |
| Co chu body | 13 pt | Normal style 12 pt; nhieu run body 12 pt/13 pt lan lon | Khong dat |
| Gian dong | 1.2 theo guideline HCMUTE 2019 | Normal style 1.35; direct formatting gom 1.0, 1.3, 1.35, 1.5 | Khong dat |
| Canh le body | Justify | Nhieu doan body justify, nhung nhieu doan ke thua alignment None | Can chuan hoa |
| Heading styles | Dung style heading that | Cac heading than bai dung Heading 1/2/3/4; front matter dung Normal | Chap nhan duoc nhung nen chuan hoa front matter |
| Page numbering | Can danh so trang | Footer khong co PAGE field; DOCX chi co 1 section | Khong dat |
| Muc luc | Can khop heading/trang | Hien la muc luc text tinh, khong co page number | Chua dat neu yeu cau muc luc tu dong/co so trang |
| Danh muc hinh/bang | Phai khop tat ca caption | Danh muc hien thieu 5 hinh va 13 bang o Chuong 1 do caption dang style Normal | Khong dat |
| Caption | Danh so theo chuong | So chuong nhin chung dung, nhung nhieu caption Chuong 1 khong dung `Report Caption` va khong vao danh muc | Can sua |
| Render visual QA | Can render/kiem tra truc quan | Khong render duoc vi thieu LibreOffice/soffice | Chua kiem duoc |

## Chi tiet phat hien tu DOCX

- DOCX co 1 section duy nhat.
- Page size: A4.
- Margin hien tai:
  - Top: 2.03 cm.
  - Bottom: 2.03 cm.
  - Left: 2.03 cm.
  - Right: 2.03 cm.
  - Header distance: 0.99 cm.
  - Footer distance: 1.27 cm.
- Footer khong co field `PAGE`/`NUMPAGES`, tuc la chua co so trang Word.
- `Normal` style:
  - Font: Times New Roman.
  - Size: 12 pt.
  - Line spacing: 1.35.
  - Space after: 6 pt.
- Direct run size thong ke:
  - 13 pt: 172 runs.
  - 12 pt: 108 runs.
  - 8 pt: 140 runs, chu yeu code blocks.
  - 10/14/16/18 pt xuat hien o caption, bia, heading/front matter.
- Code blocks dung `Consolas` 8 pt. Neu strict format "toan bo Times New Roman", can doi; neu chap nhan code block, co the giu nhung can nhat quan.

## Danh muc hinh/bang dang thieu

Danh muc hien co 22 hinh va 25 bang theo cac caption style `Report Caption`, nhung than bai co them cac caption-like paragraph dang style `Normal`.

### Hinh thieu trong danh muc

- `Hình 1.1. Sự tiến hóa từ Data Warehouse, Data Lake đến Data Lakehouse`
- `Hình 1.2. Kiến trúc Medallion với ba tầng Bronze, Silver và Gold`
- `Hình 1.3. Luồng CDC từ MongoDB qua Debezium và Kafka`
- `Hình 1.4. Cấu trúc metadata, snapshot và data file trong Apache Iceberg`
- `Hình 1.5. Vị trí PII Masking và NLP trong tầng Silver`

### Bang thieu trong danh muc

- `Bảng 1.1. So sánh Data Warehouse, Data Lake và Data Lakehouse`
- `Bảng 1.2. Vai trò các tầng dữ liệu trong kiến trúc Medallion`
- `Bảng 1.3. Vai trò các công nghệ nền tảng trong hệ thống đề tài`
- `Bảng 1.4. Phân tích logic xử lý cho kiến trúc Lakehouse cho dữ liệu Telesales`
- `Bảng 1.5. Phân tích logic xử lý cho đặc điểm dữ liệu call_logs và transcript`
- `Bảng 1.6. Phân tích logic xử lý cho tách tải phân tích khỏi MongoDB vận hành`
- `Bảng 1.7. Phân tích logic xử lý cho quản trị schema trong Lakehouse`
- `Bảng 1.8. Phân tích logic xử lý cho metadata topic, partition, offset và timestamp`
- `Bảng 1.9. Phân tích logic xử lý cho data quality qua Bronze, Silver và Gold`
- `Bảng 1.10. Phân tích logic xử lý cho bảo vệ dữ liệu cá nhân trong BI`
- `Bảng 1.12. Phân tích logic xử lý cho mô hình Star Schema`
- `Bảng 1.13. Phân tích logic xử lý cho serving view cho dashboard`
- `Bảng 1.14. Phân tích logic xử lý cho tiêu chí lựa chọn công nghệ nền`

Luu y: dang thieu `Bảng 1.11` trong chuoi danh so Chuong 1. Can kiem tra co caption bi xoa/doi ten hay danh so bi nhay.

## Uu tien sua truoc khi nop

1. Chuan hoa section/page setup:
   - A4 giu nguyen.
   - Top 3.0 cm, bottom 3.5 cm, left 3.5 cm, right 2.0 cm.
   - Header/footer 2.0 cm.
2. Chuan hoa `Normal` style:
   - Times New Roman 13 pt.
   - Line spacing 1.2 neu theo guideline HCMUTE 2019.
   - Justify body.
3. Them page numbering:
   - Can tao section/page numbering dung yeu cau khoa neu can so La Ma cho front matter va so Arab cho noi dung.
4. Chuan hoa caption:
   - Doi tat ca caption hinh/bang that sang style `Report Caption`.
   - Bo sung/tao lai danh muc hinh va danh muc bang tu tat ca caption.
   - Sua chuoi bang Chuong 1 dang nhay tu `Bảng 1.10` sang `Bảng 1.12`.
5. Cap nhat muc luc:
   - Muc luc hien la text tinh va chua co so trang.
   - Nen dung TOC field hoac it nhat them so trang sau khi render bang Word/LibreOffice.
6. Render visual QA:
   - Hien chua render duoc vi may thieu `soffice`.
   - Can mo Word/LibreOffice de kiem tra table overflow, page breaks, header/footer va muc luc.

