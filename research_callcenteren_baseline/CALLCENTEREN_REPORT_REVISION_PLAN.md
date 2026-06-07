# Ke hoach chinh sua report: CallCenterEN baseline va auxiliary training

## 1. Huong nghien cuu da chot

Dataset chinh cua do an van la tap telesales do tac gia xay dung. CallCenterEN
khong thay the dataset chinh. Vai tro cua CallCenterEN duoc chot thanh hai phan:

1. External real-world baseline: dung de doi chieu va bien minh thiet ke dataset
   telesales cua do an.
2. Auxiliary training corpus: dung mot phan transcript de ho tro domain-adaptive
   pretraining hoac pseudo-label training.

Menh de can giu nhat quan trong toan report:

```text
CallCenterEN is used as an external real-world reference baseline and auxiliary
training corpus. The proposed telesales dataset remains the primary dataset
because it contains task-specific business entities and call_code labels.
```

## 2. Co so hoc thuat can trich dan

### 2.1. CallCenterEN

Nguon:

- Dao, H., Chawla, G., Banda, R., & DeLeeuw, C. (2025). Real-World En Call
  Center Transcripts Dataset with PII Redaction. arXiv:2507.02958.
  https://arxiv.org/abs/2507.02958
- AIxBlock dataset card:
  https://huggingface.co/datasets/AIxBlock/92k-real-world-call-center-scripts-english

Noi dung can dung:

- CallCenterEN co 91,706 conversations va 10,448 audio hours.
- Du lieu la real-world English call center transcripts.
- Co inbound va outbound calls.
- Co ASR confidence, word-level timestamps, transcript text, audio duration.
- Co PII redaction.
- Duoc cong bo cho customer support va sales AI research.

Lien he voi dataset cua do an:

- `call_transcript` cua do an tuong ung transcript text.
- `talk_time_seconds` cua do an tuong ung `audio_duration`.
- PII masking o Silver layer co co so tu PII redaction trong CallCenterEN.
- Telesales transcript la mot dang ung dung gan voi customer support/sales AI.

### 2.2. Domain-adaptive pretraining

Nguon:

- Gururangan, S., Marasovic, A., Swayamdipta, S., Lo, K., Beltagy, I., Downey,
  D., & Smith, N. A. (2020). Don't Stop Pretraining: Adapt Language Models to
  Domains and Tasks. ACL 2020. https://aclanthology.org/2020.acl-main.740/

Noi dung can dung:

- Continued pretraining tren domain corpus co the giup language model thich nghi
  voi domain target.
- CallCenterEN co the duoc dung nhu domain corpus cho ngon ngu call-center.
- Dataset cua do an dung cho fine-tuning co nhan `call_code`.

### 2.3. Weak supervision va pseudo-labeling

Nguon:

- Ratner, A., De Sa, C., Wu, S., Selsam, D., & Re, C. (2016). Data Programming:
  Creating Large Training Sets, Quickly. NeurIPS 2016.
  https://papers.neurips.cc/paper/6523-data-programming-creating-large-training-sets-quickly
- Amini, M.-R., Feofanov, V., Pauletto, L., Lies Hadjadj, E., Devijver, E., &
  Maximov, Y. (2022). Self-Training: A Survey. arXiv:2202.12040.
  https://arxiv.org/abs/2202.12040

Noi dung can dung:

- Khi unlabeled data lon hon labeled data, co the dung weak supervision hoac
  pseudo-labeling.
- Label do AI sinh ra khong phai ground truth.
- Chi nen dung high-confidence pseudo-labels va can co manual review nho.

## 3. Ket qua phan tich hien co

Script da tao:

```text
research_callcenteren_baseline/analyze_dataset_baseline.py
```

Output da sinh:

```text
research_callcenteren_baseline/output/dataset_comparison_summary.json
research_callcenteren_baseline/output/dataset_comparison_table.csv
research_callcenteren_baseline/output/call_code_distribution.csv
research_callcenteren_baseline/output/report_dataset_baseline_section.md
```

Ket qua chinh:

| Metric | Dataset cua do an | CallCenterEN subset |
|---|---:|---:|
| Role | Primary task dataset | External baseline / auxiliary corpus |
| Records | 23,447 | 3,000 |
| Avg transcript chars | 897.2545 | 3,890.2737 |
| Median transcript chars | 956 | 3,843.0 |
| Avg word count | 151.0447 | 644.6783 |
| Avg duration seconds | 272.8059 | 348.4023 |
| Task labels | `call_code` | Khong co |
| ASR confidence | Khong co | 0.9534 |
| Avg PII tokens | Khong truc tiep encoded | 58.8953 |

Nhan xet can dua vao report:

- Dataset cua do an ngan va tap trung vao telesales analytics.
- CallCenterEN dai hon va gan voi call-center thuc te hon.
- Hai dataset cung co transcript va duration, nen CallCenterEN giup bien minh
  cac truong cot loi trong dataset cua do an.
- CallCenterEN khong co business entities va `call_code`, nen khong the thay
  the dataset chinh.

## 4. Chinh sua theo chuong

### Chuong 1: Motivation va problem statement

Them y:

- Public real-world call-center datasets thuong hiem do privacy, corporate
  confidentiality va compliance.
- Do an xay dung dataset telesales co nhan nghiep vu de phuc vu pipeline
  lakehouse va dashboard.
- De tranh dataset design bi xem la tuy tien, nghien cuu dung CallCenterEN lam
  external baseline doi chieu.

Doan co the dua vao:

```text
To support the validity of the proposed telesales dataset design, this thesis
uses CallCenterEN as an external real-world reference baseline. CallCenterEN
provides large-scale call-center transcripts with ASR confidence, call duration,
and PII redaction. These characteristics support the thesis decision to model
call transcripts, call duration, and privacy-preserving transformations as core
components of the lakehouse pipeline.
```

### Chuong 2: Related work

Them 3 nhom tai lieu:

1. Real-world call-center transcript dataset: CallCenterEN.
2. Domain-adaptive pretraining: Gururangan et al. 2020.
3. Weak supervision va self-training: Ratner et al. 2016, Amini et al. 2022.

### Chuong 3: Dataset and methodology

Them mot section moi:

```text
External Baseline and Auxiliary Corpus: CallCenterEN
```

Noi dung:

- Mo ta CallCenterEN.
- Mo ta subset filtering:
  - chi lay domain gan telesales: insurance, auto insurance, customer service;
  - confidence >= 0.90;
  - duration 60-900 seconds;
  - text length 300-6000 chars;
  - deduplicate theo SHA-256 hash cua normalized text.
- Dua bang so sanh dataset.
- Giai thich vi sao CallCenterEN chi la external baseline va auxiliary corpus.

### Chuong 4: System implementation

Neu report co so do pipeline NLP, them nhanh phu:

```text
CallCenterEN subset -> baseline analysis -> auxiliary pseudo-label/domain corpus
```

Khong can dua CallCenterEN vao production lakehouse neu thoi gian khong du. Neu
dua vao, chi nen dua output research vao phan experiment, khong dua vao Gold BI.

### Chuong 5: Experiments and evaluation

Them bang experiment:

| Model | Training data | Evaluation data | Purpose |
|---|---|---|---|
| M0 | Dataset cua do an | Test set cua do an | Supervised baseline |
| M1 | Dataset cua do an + preprocessing tot hon | Test set cua do an | Stronger baseline |
| M2 | CallCenterEN DAPT + dataset cua do an fine-tune | Test set cua do an | Kiem tra domain adaptation |
| M3 | Dataset cua do an + pseudo-labeled CallCenterEN subset | Test set cua do an | Kiem tra weak supervision |

Neu chua kip train M2/M3, van co the dua vao "proposed experiment". Neu da train,
bao cao metric:

- accuracy;
- micro-F1;
- macro-F1;
- per-label F1;
- confusion/error analysis theo cac nhan kho nhu `SUCCESSFUL_SALE`,
  `DO_NOT_CALL_REQUEST`, `SUDDEN_HANG_UP`.

### Conclusion

Them ket luan:

- Dataset cua do an co task-specific labels va business entities, nen phu hop
  lam primary dataset.
- CallCenterEN giup tang co so hoc thuat vi la real-world call-center corpus.
- Auxiliary corpus co the giup model hoc ngon ngu call-center thuc te, nhung
  can kiem soat domain shift va noisy pseudo-labels.

## 5. Pseudo-label workflow

Script da tao:

```text
research_callcenteren_baseline/gemini_pseudo_label_call_codes.py
```

Chay dry-run:

```powershell
python .\research_callcenteren_baseline\gemini_pseudo_label_call_codes.py --limit 50 --dry-run
```

Chay API:

```powershell
$env:GEMINI_API_KEY="YOUR_KEY_HERE"
python .\research_callcenteren_baseline\gemini_pseudo_label_call_codes.py --limit 50 --sleep 10
```

Luu y:

- API key khong duoc ghi vao file source.
- Neu gap `429 Too Many Requests`, dung chay va doi quota reset.
- Label do Gemini tao ra la pseudo-label, khong phai ground truth.
- Can manual review it nhat 30-50 mau trong pilot.

## 6. Trang thai hien tai

Da hoan thanh:

- Loc subset CallCenterEN.
- Phan tich dataset comparison.
- Tao Markdown section cho report.
- Tao pseudo-label script.
- Kiem tra dry-run prompt.

Chua hoan thanh:

- Pilot pseudo-label thanh cong, vi API hien tra `HTTP Error 429`.
- Training M3 voi pseudo-labeled subset.
- Merge section vao report DOCX chinh.

## 7. Viec tiep theo

Thu tu tiep theo nen lam:

1. Doi quota API reset hoac dung key/model khac.
2. Chay pilot 20-50 mau pseudo-label.
3. Review chat luong label.
4. Neu tot, chay 300 mau va tao quality report.
5. Train M0 vs M3 bang BoW/Logistic Regression truoc.
6. Cap nhat report voi ket qua thuc nghiem.
7. Neu con thoi gian, lam DAPT/RoBERTa nhu experiment mo rong.
