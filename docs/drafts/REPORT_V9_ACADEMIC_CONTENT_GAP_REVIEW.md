# Review noi dung hoc thuat cho bao cao v9

File duoc review: `docs/reports/Report KLTN - 22133056 - Nguyen Quoc Thinh - 27-06 - v9 academic review cleanup.docx`

Ngay review: 27/06/2026

## Nguon doi chieu

- HCMUTE FAS, `Huong dan trinh bay khoa luan tot nghiep 2019`: `https://fas.hcmute.edu.vn/Resources/Docs/SubDomain/fas/Thong%20bao/Final_Huong%20dan%20trinh%20bay%20khoa%20luan%20tot%20nghiep%202019.pdf`. Tai lieu nay neu cau truc gom bia, nhan xet GVHD/GVPB, loi cam on, tom tat tieng Viet, tom tat tieng Anh, muc luc, danh muc viet tat, danh muc bang, danh muc hinh, nhiem vu KLTN va noi dung chinh; dong thoi quy dinh danh so hinh/bang theo chuong.
- HCMUTE FAS, `Huong-dan-trinh-bay-khoa-luan-tot-nghiep-2025-KHUD.docx`: tai lieu huong dan cau truc chinh va quy cach trinh bay KLTN duoc tim thay qua cong thong tin FAS HCMUTE.
- Mau/hoc lieu ve loi hinh thuc trinh bay KLTN cua TDTU: front matter thuong co phu bia, loi cam on, loi cam doan, tom tat, muc luc, danh muc hinh, danh muc bang, danh muc thuat ngu viet tat.
- Huong dan KLTN HUB: tom tat/abstract khoang 300 tu, trinh bay co dong co so nghien cuu, muc tieu, phuong phap, ket qua va ket luan; co loi cam doan.
- Mau cau truc KLTN tong quat: mo dau can co ly do, muc tieu, doi tuong/pham vi, phuong phap, ket cau; tong quan/related work can danh gia cong trinh lien quan va khoang trong nghien cuu.

## Ket qua doi chieu nhanh

### Da co va tuong doi day du

- Bia, phieu nhan xet GVHD/GVPB, loi cam on, nhiem vu khoa luan.
- Muc luc, danh muc hinh, danh muc bang, danh muc ky hieu/chu viet tat.
- Phan mo dau co tinh cap thiet, muc dich, doi tuong/pham vi, phuong phap, cong trinh lien quan, ket qua dat duoc.
- Noi dung chinh co 5 chuong: co so ly thuyet, du lieu/mo hinh, thiet ke kien truc, trien khai-thuc nghiem-kiem thu, danh gia-han che-huong phat trien.
- Tai lieu tham khao va phu luc.
- Danh muc hinh/bang trong v9 da khop voi caption than bai.

### Thieu han nen bo sung truoc khi nop

1. `TOM TAT` tieng Viet.
   - Trang thai hien tai: khong co muc `TOM TAT`; keyword chi bat trung cac cau co chu "tom tat".
   - Ly do can bo sung: nhieu mau KLTN yeu cau tom tat/abstract truoc hoac sau loi cam doan, truoc muc luc.
   - Noi dung nen co: bai toan AGI Telesales, cach tiep can Hybrid Data Lakehouse, hai nhanh du lieu AGI Telesales va CallCenterEN, cong nghe chinh, ket qua thuc nghiem, gioi han prototype.

2. `ABSTRACT` tieng Anh.
   - Trang thai hien tai: khong co.
   - Nen viet song song voi tom tat tieng Viet, khong can dich tung cau nhung phai bao phu cung thong tin.

3. `LOI CAM DOAN` hoac `LOI CAM KET`.
   - Trang thai hien tai: khong co.
   - Nen dat trong front matter, thuong truoc hoac sau loi cam on tuy mau khoa/truong.

4. `PHIEU XAC NHAN CHINH SUA` sau bao ve.
   - Trang thai hien tai: khong co.
   - Muc nay co the la tuy khoa/hoi dong. Neu khoa yeu cau ban sau bao ve, can chen trang mau rieng.

## Co nhung con yeu, nen sua de bao cao thuyet phuc hon

1. Phan `1.4. MOT SO CONG TRINH NGHIEN CUU...` con ngan.
   - Hien chi co 3 doan, moi dung o muc mo ta chung Data Warehouse/Data Lake/Lakehouse va tinh hinh Viet Nam.
   - Nen mo rong thanh bang/danh sach doi chieu cac nhom cong trinh:
     - Lakehouse/Data Lakehouse va open table format.
     - CDC/Kafka/Debezium cho he thong phan tich gan thoi gian thuc.
     - NLP cho call transcript/call center analytics.
     - PII redaction/masking trong du lieu hoi thoai.
     - Cac he thong BI/dashboard tren Star Schema.
   - Moi nhom nen co: cong trinh/tai lieu, noi dung chinh, diem lien quan, khoang trong ma de tai xu ly.

2. Thieu cau hoi nghien cuu ro rang.
   - Hien co muc tieu va pham vi, nhung chua co cac cau hoi dang "De tai can tra loi...".
   - Nen them 3-5 cau hoi, vi du:
     - Kien truc Hybrid Data Lakehouse co tach duoc OLTP/OLAP cho AGI Telesales trong prototype khong?
     - Mo hinh Medallion xu ly duoc du lieu co cau truc va transcript phi cau truc nhu the nao?
     - BoW va RoBERTa khac nhau ra sao khi dat vao dieu kien full rebuild CPU-only?
     - CallCenterEN co the duoc to chuc thanh nhanh du lieu tuong duong de danh gia domain shift khong?

3. Thieu muc `Ket cau khoa luan`.
   - Nhieu mau KLTN co doan tom tat "Khoa luan gom N chuong...".
   - Ban hien co muc luc, nhung phan mo dau chua co doan ket cau. Nen them muc `1.6. Ket cau khoa luan`.

4. Phuong phap nghien cuu con chung.
   - Muc `1.3.3` moi noi chung la nghien cuu ly thuyet, thiet ke va thuc nghiem.
   - Nen them quy trinh thuc nghiem: nguon du lieu, split, metric NLP, tieu chi nghiem thu pipeline, run id, row count, dashboard/BigQuery validation.

5. Cac khung minh chung hinh anh van la placeholder trung tinh.
   - V9 khong con loi meta-writing, nhung trong than bai van co nhieu khung `MINH CHUNG HINH` va bang `SO DO/HINH MINH HOA`.
   - Neu nop ban cuoi, can thay bang anh that hoac so do that co nen trang, caption khop danh muc.

6. Chua co ket luan chuong rieng cho tat ca chuong.
   - Chuong 1 co `1.18. Ket luan chuong`.
   - Chuong 2, 3, 4 khong co muc ket luan chuong ro nhu chuong 1.
   - Nen them doan/muc ket luan ngan cuoi moi chuong de noi chuong do da dong gop gi cho chuong tiep theo.

7. Chua co bang doi chieu muc tieu - ket qua - minh chung.
   - Chuong 5 danh gia muc do dap ung muc tieu, nhung nen co bang tong hop:
     - Muc tieu ban dau.
     - Ket qua da lam.
     - Bang chung trong bao cao/code.
     - Han che con lai.
   - Bang nay giup hoi dong doc nhanh hon va lien ket phan mo dau voi ket luan.

8. Chua co tuyen bo ro ve dao duc du lieu/nguon du lieu.
   - Bao cao co noi PII, synthetic data va CallCenterEN, nhung nen gom lai thanh mot muc ngan:
     - Du lieu AGI Telesales la synthetic.
     - CallCenterEN la du lieu cong khai.
     - Khong dien giai dashboard synthetic nhu ket luan kinh doanh that.
     - PII chi duoc dung trong pham vi pipeline va duoc masking/drop o serving layer.

## Uu tien sua

### Bat buoc/uu tien cao

1. Them `TOM TAT`.
2. Them `ABSTRACT`.
3. Them `LOI CAM DOAN`/`LOI CAM KET`.
4. Mo rong related work va khoang trong nghien cuu.
5. Thay khung minh chung bang hinh/so do that neu day la ban nop chinh thuc.

### Uu tien trung binh

1. Them cau hoi nghien cuu.
2. Them ket cau khoa luan.
3. Them ket luan chuong 2, 3, 4.
4. Them bang doi chieu muc tieu - ket qua - minh chung.
5. Viet ro muc dao duc du lieu/nguon du lieu.

### Co the lam sau

1. Them phieu xac nhan chinh sua neu khoa/hoi dong yeu cau.
2. Them bang artifact reproducibility co run id, file output, commit/checksum neu can tang tinh kiem chung.
