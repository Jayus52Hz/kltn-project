# Draft nội dung chỉnh sửa report - BoW là mô hình chính

File này là bản nháp để rà soát trước khi đưa vào report chính. Nội dung đã được chỉnh lại theo kết quả thực nghiệm full rebuild ngày 01/06/2026: RoBERTa được giữ như mô hình baseline/đối chứng học sâu, còn mô hình vận hành chính của pipeline là Bag-of-Words kết hợp Logistic Regression theo chiến lược One-vs-Rest.

## 1. Nội dung đề xuất đưa vào Chương 1: Cơ sở lý thuyết

### 1.1. Bài toán nghiệp vụ và phạm vi phân tích

Hệ thống lakehouse trong đề tài không được thiết kế để "giải quyết cả một lakehouse" theo nghĩa lưu trữ tất cả dữ liệu không có mục tiêu. Phạm vi chính của hệ thống là giải quyết bài toán phân tích hiệu quả cuộc gọi telesales. Từ transcript cuộc gọi, thông tin khách hàng, offer và chiến dịch, hệ thống cần chuyển nội dung hội thoại thành các mã nghiệp vụ có cấu trúc (`call_code`), sau đó phục vụ dashboard và phân tích vận hành.

Các câu hỏi nghiệp vụ trọng tâm gồm:

- Chiến dịch, sản phẩm hoặc nguồn lead nào tạo tỷ lệ bán hàng thành công cao hơn?
- Nhóm khách hàng nào thường xuất hiện phản đối, trì hoãn, yêu cầu follow-up hoặc yêu cầu không gọi lại?
- Agent nào có hiệu quả tốt hơn theo số cuộc gọi, thời lượng đàm thoại, outcome và tỷ lệ chuyển đổi?
- Những mã hội thoại nào thường đi cùng nhau, ví dụ `PRODUCT_PITCH` với `OBJECTION_HANDLING` hoặc `WARM_LEAD`?
- Xu hướng outcome theo ngày/tháng thay đổi như thế nào?

Vì vậy, Gold layer và BigQuery chỉ giữ dữ liệu đã cấu trúc hóa cho phân tích: khóa định danh, chiều khách hàng/sản phẩm/thời gian, trạng thái cuộc gọi, `talk_time_seconds`, `call_code` do model sinh và các cờ outcome. Transcript không được đưa lên Gold/BigQuery vì không phải trường phân tích trực tiếp và có thể chứa PII.

Code minh chứng ranh giới dữ liệu:

```python
BLOCKED_BIGQUERY_COLUMNS = {
    "call_transcript",
    "call_code_original",
    "call_code_predicted",
    "full_name",
    "address",
}
```

### 1.2. Vai trò của `call_code` trong training và production

Trong dữ liệu ban đầu, `call_code` được sinh cùng transcript để tạo nhãn huấn luyện cho mô hình NLP. Nhãn này chỉ đóng vai trò ground truth trong giai đoạn training, validation và testing. Sau khi mô hình đã được huấn luyện, pipeline vận hành không dùng lại `call_code` gốc. Source of truth cho các bước phân tích sau là `call_code` do mô hình dự đoán từ `call_transcript`.

Thiết kế này mô phỏng đúng bối cảnh doanh nghiệp: khi có cuộc gọi mới, doanh nghiệp thường có transcript hoặc log cuộc gọi, nhưng không có sẵn nhãn nghiệp vụ. Hệ thống NLP phải sinh ra `call_code`, sau đó lakehouse mới dùng `call_code` đó để tính KPI.

Code minh chứng ở bước load dữ liệu vào MongoDB:

```python
# Source call_code is a synthetic training label only.
if "call_code" in df.columns:
    df = df.drop(columns=["call_code"])
```

Code minh chứng ở Silver layer:

```python
silver_calls = (
    parsed_calls
    .withColumn("call_code", predict_call_codes(F.col("call_transcript")))
)
```

### 1.3. Cơ sở lý thuyết bài toán phân loại đa nhãn

Bài toán gán `call_code` là bài toán multi-label classification. Khác với multi-class classification, trong đó mỗi mẫu chỉ thuộc một lớp duy nhất, multi-label classification cho phép một transcript đồng thời mang nhiều nhãn. Ví dụ một cuộc gọi có thể vừa chứa `OPENING`, vừa có `PRODUCT_PITCH`, vừa xuất hiện `OBJECTION_HANDLING` và kết thúc bằng `FOLLOW_UP_EMAIL_REQUESTED`.

Về mặt biểu diễn dữ liệu, mỗi transcript được ánh xạ thành một vector nhị phân nhiều chiều. Nếu hệ thống có 32 call codes, vector nhãn cũng có 32 phần tử. Giá trị `1` tại một vị trí nghĩa là transcript có nhãn tương ứng; giá trị `0` nghĩa là không có nhãn đó. Trong notebook, bước này được thực hiện bằng `MultiLabelBinarizer` của scikit-learn.

Code minh chứng:

```python
mlb.fit(full_train_df["call_code_list"])
y_train = mlb.transform(train_df["call_code_list"]).astype("float32")
```

Ý nghĩa của cách biểu diễn này là mô hình không bị ép chọn một nhãn duy nhất. Thay vào đó, mỗi nhãn được xem như một quyết định nhị phân riêng: có hoặc không có nhãn đó trong transcript.

### 1.4. Mô hình chính: Bag-of-Words + Logistic Regression

#### 1.4.1. Nguồn gốc và thư viện sử dụng

Mô hình chính của hệ thống là Bag-of-Words kết hợp Logistic Regression theo chiến lược One-vs-Rest. Toàn bộ mô hình được triển khai bằng scikit-learn, gồm ba thành phần chính:

- `CountVectorizer`: chuyển transcript từ văn bản thô thành vector tần suất token/ngram.
- `LogisticRegression`: học bộ phân loại tuyến tính cho từng nhãn.
- `OneVsRestClassifier`: biến bài toán multi-label thành nhiều bài toán binary classification độc lập.

Artifact mô hình được lưu tại `NLP model/models/bow_model.pkl` dưới dạng dictionary gồm `vectorizer`, `classifier` và `mlb`. Trong pipeline Silver, artifact này được nạp bằng `joblib.load()` và broadcast cho Spark executors để chạy inference.

Code minh chứng từ artifact:

```python
bundle = joblib.load(BOW_MODEL_PATH)
X = bundle["vectorizer"].transform(transcripts.fillna(""))
y_pred = bundle["classifier"].predict(X)
label_list = bundle["mlb"].inverse_transform(y_pred)
```

#### 1.4.2. Cách hoạt động của Bag-of-Words

Bag-of-Words biểu diễn văn bản bằng cách đếm sự xuất hiện của các token hoặc cụm token, thay vì cố gắng mô hình hóa thứ tự đầy đủ của câu. Với mỗi transcript, `CountVectorizer` thực hiện các bước:

1. Chuẩn hóa chữ thường.
2. Tách transcript thành token theo `token_pattern`.
3. Loại bỏ stop words tiếng Anh.
4. Sinh unigram và bigram theo `ngram_range=(1, 2)`.
5. Xây dựng vocabulary từ tập train.
6. Biến mỗi transcript thành vector sparse có số chiều bằng kích thước vocabulary.

Trong đề tài, cấu hình cụ thể là:

- `max_features=5000`
- `ngram_range=(1, 2)`
- `stop_words="english"`
- `lowercase=True`
- vocabulary thực tế: 5,000 đặc trưng

Code minh chứng trong notebook:

```python
counter = CountVectorizer(
    max_features=5000,
    ngram_range=(1, 2),
    stop_words="english",
)
X_train_count = counter.fit_transform(full_train_df["call_transcript"])
```

Việc dùng unigram giúp mô hình nhận diện các từ khóa đơn như "cancel", "interested", "expensive", "email". Việc dùng bigram giúp mô hình nhận diện cụm có ý nghĩa hơn như "call back", "not interested", "send email", "too expensive". Đây là điểm quan trọng trong transcript telesales vì nhiều intent nghiệp vụ không nằm ở một từ đơn lẻ mà nằm ở cụm từ ngắn.

Bag-of-Words không hiểu ngữ cảnh sâu như Transformer, nhưng đổi lại có ba ưu điểm lớn cho bài toán này:

- Huấn luyện và suy luận rất nhanh.
- Dễ giải thích vì feature là token/ngram cụ thể.
- Triển khai nhẹ trong Spark, không cần GPU, không cần nạp mô hình vài trăm MB.

#### 1.4.3. Logistic Regression cho từng nhãn

Logistic Regression là mô hình tuyến tính cho phân loại. Với mỗi nhãn, mô hình học một vector trọng số `w` và bias `b`. Khi nhận vector đặc trưng `x` từ CountVectorizer, mô hình tính:

```text
z = w.x + b
p = sigmoid(z)
```

Trong đó `p` là xác suất transcript có nhãn đang xét. Nếu `p` vượt ngưỡng quyết định, nhãn được gán cho transcript. Do input là vector BoW sparse, trọng số của Logistic Regression có thể hiểu như mức đóng góp của từng token/ngram vào khả năng xuất hiện của một call code.

Notebook dùng `solver="liblinear"` và `class_weight="balanced"`. Tham số `class_weight="balanced"` quan trọng vì các call codes không xuất hiện đồng đều; một số nhãn như `OPENING` hoặc `PRODUCT_PITCH` phổ biến hơn nhiều so với nhãn hiếm như `SUCCESSFUL_SALE` hoặc `CLOSING_NEGOTIATION`. Cân bằng trọng số giúp mô hình không quá thiên về các lớp phổ biến.

Code minh chứng:

```python
base_lr = LogisticRegression(
    solver="liblinear",
    class_weight="balanced",
    random_state=42,
)
```

#### 1.4.4. One-vs-Rest cho multi-label classification

`OneVsRestClassifier` dùng chiến lược binary relevance: với mỗi nhãn, hệ thống huấn luyện một classifier nhị phân riêng. Nếu có 32 call codes, mô hình sẽ có 32 Logistic Regression estimators. Mỗi estimator trả lời một câu hỏi: transcript này có nhãn X hay không?

Code minh chứng:

```python
counting_model = OneVsRestClassifier(base_lr)
counting_model.fit(X_train_count, y_train_full)
```

Cách tiếp cận này phù hợp với dữ liệu `call_code` vì nhiều nhãn có thể đồng thời đúng. Nó cũng giúp pipeline suy luận đơn giản: sau khi `classifier.predict(X)` trả về ma trận nhị phân, `MultiLabelBinarizer.inverse_transform()` chuyển ma trận đó về danh sách call codes.

Code minh chứng trong Silver:

```python
y_pred = bundle["classifier"].predict(X)
label_list = bundle["mlb"].inverse_transform(y_pred)
return pd.Series([list(labels) for labels in label_list])
```

#### 1.4.5. Lý do chọn BoW làm mô hình chính

Ban đầu RoBERTa được cân nhắc làm mô hình chính vì Precision trên test set cao hơn. Tuy nhiên, sau khi thực nghiệm full rebuild trong môi trường lakehouse thực tế, chi phí vận hành của RoBERTa quá lớn so với mức cải thiện chất lượng. Trong khi đó, BoW có F1 Micro chỉ thấp hơn 3.30 điểm phần trăm, Recall cao hơn, Exact Match tương đương, và đặc biệt inference nhanh hơn rất nhiều.

Kết quả test set:

| Model | Vai trò sau thực nghiệm | F1 Micro | F1 Macro | Precision | Recall | Exact Match |
|---|---|---:|---:|---:|---:|---:|
| CountVectorizer + Logistic Regression | Mô hình chính production | 70.16% | 62.08% | 65.92% | 74.98% | 17.89% |
| RoBERTa fine-tuned | Baseline học sâu/đối chứng | 73.46% | 56.14% | 80.39% | 67.62% | 17.71% |

Về mặt nghiệp vụ, Precision của RoBERTa cao hơn là điểm tốt, nhưng hệ thống lakehouse cần chạy ổn định end-to-end. Nếu một bước NLP mất quá lâu hoặc dễ crash, toàn bộ pipeline phân tích bị chặn. Vì vậy đề tài chọn BoW làm mô hình chính do cân bằng tốt hơn giữa chất lượng, tốc độ, khả năng giải thích và chi phí vận hành.

### 1.5. RoBERTa như baseline học sâu

RoBERTa vẫn được trình bày trong report nhưng với vai trò baseline học sâu, không còn là mô hình production. Về nguồn gốc, RoBERTa là biến thể được tối ưu từ BERT, được công bố trong paper "RoBERTa: A Robustly Optimized BERT Pretraining Approach". Mô hình kế thừa kiến trúc Transformer encoder, dùng self-attention để học quan hệ ngữ cảnh giữa các token trong chuỗi.

Trong notebook, RoBERTa được triển khai bằng Hugging Face `transformers`:

- `AutoTokenizer.from_pretrained("roberta-base")`
- `AutoModelForSequenceClassification.from_pretrained(...)`
- `Trainer` và `TrainingArguments`
- `BCEWithLogitsLoss` cho multi-label classification

Code minh chứng:

```python
model = AutoModelForSequenceClassification.from_pretrained(
    "roberta-base",
    num_labels=num_labels,
    problem_type="multi_label_classification",
)
```

RoBERTa token hóa transcript bằng tokenizer của `roberta-base`, giới hạn `max_length=512`, chạy qua các tầng Transformer encoder, sinh logits cho 32 nhãn, rồi dùng sigmoid để quyết định từng nhãn độc lập.

Code inference:

```python
probs = torch.sigmoid(_roberta_model(**encoded).logits).cpu().numpy()
pred = [labels[i] for i, score in enumerate(row) if score >= ROBERTA_THRESHOLD]
```

RoBERTa có lợi thế về hiểu ngữ cảnh và giảm false positive, nhưng bất lợi lớn trong môi trường vận hành của đề tài là kích thước artifact và chi phí suy luận. Checkpoint `roberta_saved/model.safetensors` khoảng 498 MB; khi chạy trong Spark CPU-only, mỗi Python worker phải nạp PyTorch, tokenizer và model, làm thời gian chạy tăng mạnh và dễ gây lỗi worker.

### 1.6. Kết luận lựa chọn mô hình

Quyết định cuối cùng của đề tài là:

- BoW + Logistic Regression là mô hình chính để sinh `call_code` trong Silver.
- RoBERTa là baseline học sâu để so sánh chất lượng, trình bày lý thuyết và phân tích trade-off.
- `call_code` gốc chỉ dùng cho training/testing, không đi vào pipeline production.
- `call_code` trong Silver, Gold và BigQuery là output của model.

Code cấu hình production:

```python
NLP_MODEL_TYPE = os.getenv("NLP_MODEL_TYPE", "bow").lower()
```

## 2. Nội dung đề xuất đưa vào Chương 2: Thiết kế và thực nghiệm hệ thống

### 2.1. Luồng dữ liệu production

Pipeline production gồm các bước:

1. Load dữ liệu vào MongoDB nhưng loại bỏ `call_code` gốc.
2. Debezium đọc CDC từ MongoDB và đẩy sang Kafka.
3. Bronze ingest raw CDC event vào Iceberg.
4. Silver parse JSON, deduplicate, mask PII và chạy NLP inference.
5. Gold xây star schema và derive các cờ outcome từ `call_code`.
6. BigQuery sync Gold tables phục vụ BI.

Điểm quan trọng là transcript chỉ tồn tại đến Silver để phục vụ inference. Gold và BigQuery không giữ transcript.

### 2.2. Tích hợp BoW vào Silver layer

Silver job nạp artifact `bow_model.pkl`, broadcast bundle model và chạy `predict_call_codes()` trên `call_transcript`.

Code minh chứng:

```python
print(f"Using BoW production model from {BOW_MODEL_PATH} ...")
bow_broadcast = spark.sparkContext.broadcast(joblib.load(BOW_MODEL_PATH))
```

```python
@F.pandas_udf(ArrayType(StringType()))
def predict_call_codes(transcripts: pd.Series) -> pd.Series:
    bundle = bow_broadcast.value
    X = bundle["vectorizer"].transform(transcripts.fillna(""))
    y_pred = bundle["classifier"].predict(X)
    label_list = bundle["mlb"].inverse_transform(y_pred)
    return pd.Series([list(labels) for labels in label_list])
```

Airflow DAG cũng được đổi sang BoW mặc định:

```python
env_vars={
    "MODELS_PATH": "/opt/spark/work-dir/batch-etl/models",
    "NLP_MODEL_TYPE": "bow",
}
```

### 2.3. Thiết kế Silver, Gold và BigQuery theo yêu cầu bảo vệ PII

Silver giữ transcript vì đây là input cần thiết cho mô hình. Gold không giữ transcript, không giữ `call_code_original`, không giữ `call_code_predicted`; chỉ giữ `call_code` do model sinh.

Kết quả kiểm tra schema sau rebuild:

```text
lakehouse.silver.call_logs:
  rows = 23447
  has call_code = true
  has call_transcript = true
  has call_code_original = false
  has call_code_predicted = false

lakehouse.gold.fact_telesales_calls:
  rows = 23447
  has call_code = true
  has call_transcript = false
  has call_code_original = false
  has call_code_predicted = false
```

Gold derive outcome từ `call_code`:

```python
F.array_contains(F.col("call_code"), "SUCCESSFUL_SALE").alias("has_successful_sale")
```

```python
F.when(F.array_contains(F.col("call_code"), "SUCCESSFUL_SALE"), "SALE")
 .when(F.array_contains(F.col("call_code"), "DO_NOT_CALL_REQUEST"), "DO_NOT_CALL")
 .otherwise("IN_PROGRESS")
 .alias("outcome_category")
```

### 2.4. Thực nghiệm full rebuild và quyết định đổi mô hình

Trong quá trình full rebuild ngày 01/06/2026, pipeline được chạy lại từ đầu với Docker volumes sạch. Dữ liệu nguồn load vào MongoDB gồm:

- `customers`: 4,344 bản ghi
- `offers`: 5,072 bản ghi
- `call_logs`: 23,447 bản ghi

Kiểm tra MongoDB cho thấy `call_logs` không còn field `call_code`; điều này xác nhận `call_code` gốc chỉ phục vụ training:

```text
has_call_code = false
calls_count = 23447
```

Kết quả thực nghiệm RoBERTa:

| Lần chạy | Cấu hình | Kết quả |
|---|---|---|
| RoBERTa attempt 1 | `batch_size=16`, 1 Spark task inference | Chạy từ 05:53:48 đến 06:10:07, sau hơn 16 phút vẫn chưa qua Silver nên phải dừng. Spark UI cho thấy stage đọc 23,447 records nhưng chỉ có 1 task active. |
| RoBERTa attempt 2 | `partitions=4`, `batch_size=16`, `torch_threads=1` | Tăng được CPU lên khoảng 380%, nhưng Spark/Python worker crash. Log báo `Python worker exited unexpectedly`, `EOFException`, `spark-submit` exit code `-9`. |
| RoBERTa attempt 3 | `partitions=2`, `batch_size=8`, `torch_threads=1` | Ổn định hơn nhưng vẫn chạy rất lâu; đến 07:24:59 vẫn chưa hoàn tất Silver nên dừng để đổi mô hình. |

Kết quả thực nghiệm BoW:

| Task | Thời gian chạy | Kết quả |
|---|---:|---|
| Silver với BoW | 07:27:32 -> 07:28:12 | Success, merge 23,447 `call_logs` |
| Gold | 07:28:14 -> 07:28:42 | Success |
| BigQuery sync | 07:32:36 -> 07:33:15 | Success, sync 23,447 fact rows |

Log Silver với BoW:

```text
Using BoW production model from /opt/spark/work-dir/batch-etl/models/bow_model.pkl
MERGE INTO lakehouse.silver.call_logs completed (23,447 source records)
Silver job completed successfully.
```

Kết luận thực nghiệm: RoBERTa có Precision cao hơn trên test set, nhưng trong full rebuild CPU-only nó quá nặng và không mang lại cải thiện đủ rõ rệt so với chi phí vận hành. BoW chạy nhanh, ổn định và đủ tốt cho mục tiêu phân tích dashboard, nên phù hợp hơn làm mô hình chính của hệ thống.

### 2.5. Điều chỉnh BigQuery sync sau khi đổi schema

Khi `fact_telesales_calls` bắt đầu expose cột `call_code`, BigQuery table cũ có thể chưa có cột này. Lần sync đầu tiên bị lỗi schema mismatch:

```text
INVALID_ARGUMENT: Input schema has more fields than BigQuery schema,
extra fields: 'call_code'
```

Để xử lý schema evolution trong full sync, job BigQuery được chỉnh để xóa table cũ trước khi ghi lại bằng schema mới. Đây là cách phù hợp vì các bảng Gold đang được sync theo chế độ overwrite serving, không phải incremental append.

Code minh chứng:

```python
bq_client.delete_table(target, not_found_ok=True)
writer.save(target)
```

Sau khi chỉnh, BigQuery sync thành công:

```text
Synced lakehouse.gold.fact_telesales_calls -> ...fact_telesales_calls (23,447 rows)
BigQuery sync completed successfully.
```

## 3. Tài liệu tham khảo đề xuất

- scikit-learn `CountVectorizer`: https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html
- scikit-learn `LogisticRegression`: https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html
- scikit-learn `OneVsRestClassifier`: https://scikit-learn.org/stable/modules/generated/sklearn.multiclass.OneVsRestClassifier.html
- scikit-learn multiclass/multilabel algorithms: https://scikit-learn.org/stable/modules/multiclass.html
- scikit-learn `MultiLabelBinarizer`: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MultiLabelBinarizer.html
- RoBERTa paper: https://arxiv.org/abs/1907.11692
- Hugging Face RoBERTa documentation: https://huggingface.co/docs/transformers/main/model_doc/roberta
- Hugging Face `AutoModelForSequenceClassification`: https://huggingface.co/docs/transformers/main/model_doc/auto
- PyTorch `BCEWithLogitsLoss`: https://docs.pytorch.org/docs/stable/generated/torch.nn.BCEWithLogitsLoss.html

## 4. Checklist đưa vào report chính

- Chương 1 cần đổi trọng tâm mô hình từ RoBERTa sang BoW + Logistic Regression.
- RoBERTa vẫn được trình bày, nhưng là baseline học sâu/đối chứng thực nghiệm.
- Chương 2 cần nêu rõ thực nghiệm full rebuild: RoBERTa chậm/crash, BoW chạy thành công end-to-end.
- Không dùng `call_code_original` trong production, Gold, BigQuery hoặc dashboard.
- Không đưa transcript lên Gold/BigQuery.
- Không đề cập việc dùng AI để viết report.
