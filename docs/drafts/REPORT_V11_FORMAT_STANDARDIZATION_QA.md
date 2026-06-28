# QA dinh dang bao cao v11

File QA: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v11 format standardization.docx`

Ngay QA: 27/06/2026

## Thay doi da thuc hien

- Tao ban v11 tu v10, khong ghi de ban v10.
- Chuan hoa page setup:
  - A4: 21.0 x 29.7 cm.
  - Le tren: 3.0 cm.
  - Le duoi: 3.5 cm.
  - Le trai: 3.5 cm.
  - Le phai: 2.0 cm.
  - Header/footer distance: 2.0 cm.
- Chuan hoa style:
  - `Normal`: Times New Roman, 13 pt, justify, line spacing 1.2.
  - `Heading 1`: Times New Roman, 16 pt, bold, centered, line spacing 1.2.
  - `Heading 2/3/4`: Times New Roman, 13 pt, bold, line spacing 1.2.
  - `Report Caption`: Times New Roman, 13 pt, italic, centered, line spacing 1.2.
- Them Word `PAGE` field vao footer.
- Doi tat ca caption hinh/bang that trong than bai sang style `Report Caption`.
- Renumber caption theo chuong thuc te.
- Rebuild `DANH MUC HINH ANH` va `DANH MUC BANG` tu toan bo caption trong than bai.
- Sua chuoi bang Chuong 1 bi nhay so: nay chay lien tuc tu `Bang 1.1` den `Bang 1.13`.
- Bat `w:updateFields=true` de Word co the cap nhat field khi mo file.

## Ket qua QA cau truc

- DOCX mo duoc bang `python-docx`.
- Paragraph count: 1,148.
- Table count: 74.
- Page setup dat cac thong so le theo guideline da doi chieu.
- `Normal` style: Times New Roman, 13 pt, line spacing 1.2, justify.
- Footer co `PAGE` field.
- `DANH MUC HINH ANH`: 27 muc, khop 27 caption hinh trong than bai.
- `DANH MUC BANG`: 38 muc, khop 38 caption bang trong than bai.
- Tat ca caption hinh/bang trong than bai dung style `Report Caption`.
- Chuoi bang Chuong 1: `Bang 1.1` den `Bang 1.13`, khong con thieu `Bang 1.11`.

## Gioi han con lai

- Muc luc van la text tinh va chua co so trang. De co muc luc tu dong/co so trang chinh xac, can mo bang Microsoft Word/LibreOffice va cap nhat/generate TOC sau khi layout on dinh.
- Footer `PAGE` la Word field; khi mo file Word co the can chap nhan update fields neu duoc hoi.
- Visual render QA chua hoan tat duoc vi moi truong Windows hien tai khong co LibreOffice/`soffice`, renderer bao `FileNotFoundError: [WinError 2]`.
- Vi tang font va margin theo chuan, can mo bang Word de kiem tra bang rong/table overflow bang mat thuong truoc khi nop ban cuoi.

