# 🚀 Hướng Dẫn Chạy Project YouTube Trending

## 📋 Yêu Cầu Chuẩn Bị

### Chuẩn Bị Trước
1. **Python 3.7+** đã cài đặt
2. **Git** để quản lý code
3. **Dữ liệu CSV** từ YouTube (hoặc tải từ Kaggle)

### Cài Đặt Môi Trường

**Bước 1: Clone project (nếu chưa có)**
```bash
cd ~/Documents
git clone <repo-url>
cd DS200.M21-Big-Data/FINAL\ PROJECT
```

**Bước 2: Tạo virtual environment**
```bash
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# Hoặc trên Windows: .venv\Scripts\activate
```

**Bước 3: Cài đặt dependencies**
```bash
pip install -r requirements.txt
```

---

## 🎯 4 Cách Chạy Project

### ✅ **Cách 1: Chạy Spark Local (Nhanh nhất, không cần HDFS)**

**Khi nào dùng:** Thử nghiệm nhanh, máy không có Hadoop

**Lệnh:**
```bash
bash scripts/run_spark.sh "kaggle_youtube/trending_yt_videos_113_countries.csv" \
  --no-sample \
  --num-trees 100 \
  --max-depth 12 \
  --save-metrics /tmp/rf_metrics.json
```

**Kết quả:**
- ✓ Model training xong trong ~30 giây
- ✓ In ra feature importance
- ✓ Tạo sample predictions
- ✓ Lưu metrics vào `/tmp/rf_metrics.json`

**Kiểm tra kết quả:**
```bash
cat /tmp/rf_metrics.json
```

---

### ✅ **Cách 2: Chạy Spark + Lưu Visualizations (Khuyên dùng)**

**Khi nào dùng:** Muốn xem biểu đồ model performance

**Lệnh:**
```bash
bash scripts/run_spark.sh "kaggle_youtube/trending_yt_videos_113_countries.csv" \
  --no-sample \
  --num-trees 100 \
  --max-depth 12 \
  --save-visualizations Images
```

**Kết quả:**
- ✓ Tạo 4 biểu đồ trong thư mục `Images/`:
  - `feature_importance.png` — Tầm quan trọng các feature
  - `model_metrics.png` — AUC score
  - `data_engagement_dist.png` — Phân bố engagement
  - `data_like_ratio_dist.png` — Phân bố like ratio

**Xem biểu đồ:**
```bash
# Linux/Mac
open Images/feature_importance.png

# Hoặc mở thư mục Images/ bằng file manager
```

---

### ✅ **Cách 3: Chạy Full HDFS + Spark (Big Data, khuyên nhất)**

**Khi nào dùng:** Muốn chạy trên Hadoop HDFS, lưu model + predictions

**Bước 1: Kiểm tra dữ liệu**
```bash
# Nếu dữ liệu từ Windows, upload qua SCP:
# Trên Windows PowerShell:
pwsh
.\app\scripts\scp_upload_raw.ps1 `
  -LocalCsv "C:\Users\YourUser\.cache\kagglehub\...\trending_yt_videos_113_countries.csv" `
  -VmUser thinh `
  -VmHost 10.0.2.15

# Hoặc copy file thủ công vào /home/thinh/data/
```

**Bước 2: Chạy orchestration script (trên VM Linux)**
```bash
cd /home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL\ PROJECT
sudo bash app/scripts/orchestrate_full_pipeline.sh \
  /home/thinh/data/trending_yt_videos_113_countries.csv \
  --hdfs-user thinh \
  --num-trees 100 \
  --max-depth 12
```

**Kết quả:**
- ✓ Hadoop HDFS được thiết lập (nếu chưa có)
- ✓ File CSV được đẩy vào HDFS: `/user/thinh/input/`
- ✓ Spark chạy trên HDFS
- ✓ Model lưu vào: `/user/thinh/models/rf/` (HDFS)
- ✓ Predictions lưu vào: `/user/thinh/predictions/rf/` (HDFS)
- ✓ Metrics lưu vào: `/tmp/rf_metrics.json` (local)

**Kiểm tra HDFS:**
```bash
# Xem dữ liệu input
hdfs dfs -ls /user/thinh/input

# Xem model
hdfs dfs -ls /user/thinh/models/rf

# Xem predictions
hdfs dfs -ls /user/thinh/predictions/rf

# Xem metrics
cat /tmp/rf_metrics.json
```

---

### ✅ **Cách 4: Jupyter Notebook (Phân tích chi tiết)**

**Khi nào dùng:** Muốn xem từng bước, trực quan hóa dữ liệu

**Lệnh:**
```bash
jupyter notebook "Predict YouTube Trending.ipynb"
```

**Kết quả:**
- ✓ Mở Jupyter tại http://localhost:8888
- ✓ Chạy từng cell để xem quá trình
- ✓ Xem EDA (exploratory data analysis)
- ✓ So sánh 4 models khác nhau
- ✓ Tạo visualizations chi tiết

---

## ⚙️ Unit Tests (Tùy chọn)

**Chạy tất cả tests:**
```bash
pip install pytest
python -m pytest app/tests/ -v
```

**Chạy test riêng:**
```bash
# Test data cleaning
python -m pytest app/tests/test_clean_spark.py -v

# Test model training
python -m pytest app/tests/test_train_spark.py -v
```

**Kết quả:**
- ✓ Kiểm tra data loading, feature engineering, model training
- ✓ Mỗi test mất ~5-10 giây

---

## 🐛 Khắc Phục Sự Cố

### ❌ Lỗi: `ModuleNotFoundError: No module named 'pyspark'`
**Giải pháp:**
```bash
pip install pyspark
```

### ❌ Lỗi: `No module named 'matplotlib'`
**Giải pháp:** (nếu muốn visualizations)
```bash
pip install matplotlib seaborn
```

### ❌ Lỗi: File CSV không tìm thấy
**Giải pháp:**
```bash
# Kiểm tra đường dẫn file
ls -lh /path/to/your/data.csv

# Hoặc cất file trong thư mục project
cp ~/Downloads/trending_yt_videos_113_countries.csv ./kaggle_youtube/
```

### ❌ Lỗi: `JAVA_HOME not set` (khi chạy Spark)
**Giải pháp:**
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

### ❌ Lỗi: Hadoop/HDFS không tìm thấy (khi chạy orchestrate)
**Giải pháp:** Script orchestration sẽ tự cài Hadoop nếu chưa có (với `sudo`)
```bash
sudo bash app/scripts/orchestrate_full_pipeline.sh ...
```

---

## 📊 Ví Dụ Output

### Khi chạy Cách 1 (Spark Local)
```
Loading CSVs matching: kaggle_youtube/trending_yt_videos_113_countries.csv
Loaded 500000 rows and 18 columns
✓ Normalized columns: ['tag_count', 'description_length', ...]

=== Stage 2: Feature Engineering ===
✓ Engineered features: ['tag_count', 'description_length', 'like_ratio', 'comment_ratio', 'engagement']

=== Stage 3: Model Training ===
Median engagement (approx): 25000.0
Training on 200000 rows, testing on 50000 rows (no sampling)
Model AUC on test set: 0.8723

Feature importances:
  tag_count: 0.1095
  description_length: 0.2041
  like_ratio: 0.1962
  comment_ratio: 0.1744
  engagement: 0.3158

=== Stage 6: Sample Predictions ===
+----------+-------------------+----------+---------------+----------+----------+-----------+
|tag_count |description_length |like_ratio|comment_ratio  |engagement|prediction|probability|
+----------+-------------------+----------+---------------+----------+----------+-----------+
|40        |800                |0.10      |0.05           |50000     |1         |[0.1, 0.9]|
|5         |100                |0.01      |0.001          |100       |0         |[0.8, 0.2]|
|25        |400                |0.08      |0.03           |20000     |1         |[0.2, 0.8]|
+----------+-------------------+----------+---------------+----------+----------+-----------+

✓ Pipeline completed successfully!
```

---

## 🎓 Workflow Tổng Quát

```
1. Chuẩn bị dữ liệu
   ↓
2. Chọn cách chạy (Local / HDFS / Jupyter)
   ↓
3. Chạy script tương ứng
   ↓
4. Xem kết quả:
   - Metrics: /tmp/rf_metrics.json
   - Visualizations: Images/
   - Model: /user/thinh/models/rf/ (nếu HDFS)
   - Predictions: /user/thinh/predictions/rf/ (nếu HDFS)
   ↓
5. Phân tích kết quả
```

---

## 💡 Mẹo & Khuyến Nghị

1. **Thử Cách 1 trước** (Spark Local) để kiểm tra data + code hoạt động
2. **Dùng Cách 2 nếu muốn biểu đồ** (thêm `--save-visualizations Images`)
3. **Dùng Cách 3 cho production** (HDFS + model persistence)
4. **Dùng Cách 4 để học & phân tích** (Jupyter + EDA)

---

## 🔗 Liên Kết Nhanh

- **Tài liệu chi tiết:** [README_YOUTUBE.md](README_YOUTUBE.md)
- **HDFS setup:** [app/scripts/README_HDFS_SETUP.md](app/scripts/README_HDFS_SETUP.md)
- **Testing:** [app/tests/README_TESTS.md](app/tests/README_TESTS.md)

---

## ❓ Câu Hỏi Thường Gặp

**Q: Mất bao lâu để chạy?**
- Cách 1 (Local): ~30 giây
- Cách 2 (Local + Viz): ~45 giây
- Cách 3 (HDFS): 1-2 phút (lần đầu)
- Cách 4 (Jupyter): Tùy bạn

**Q: Cần GPU không?**
A: Không cần. CPU đủ dùng.

**Q: Data bao lớn?**
A: 1M+ video từ 113 quốc gia (~10GB), nhưng script có thể xử lý từ 100KB đến 100GB.

**Q: Model nào tốt nhất?**
A: RandomForest với AUC ~0.87 (87% accuracy).

**Q: Có thể chạy trên Windows không?**
A: Có, nhưng khuyên dùng Linux/Mac. Hoặc chạy trên Lubuntu VM.

---

**Bắt đầu ngay!** Chạy Cách 1 trước:
```bash
bash scripts/run_spark.sh "kaggle_youtube/trending_yt_videos_113_countries.csv" --no-sample
```
