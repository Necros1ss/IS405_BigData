# 🚀 Hướng Dẫn Chạy Project YouTube Trending

## 📋 Yêu Cầu Chuẩn Bị

### Chuẩn Bị Trước
1. Python 3.7+ đã cài đặt
2. Git để quản lý code
3. Dataset CSV cục bộ trong thư mục `data/` (mặc định: `data/*videos.csv`)

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

### ✅ Cách 1: Chạy Spark Local (Nhanh nhất, không cần HDFS)

**Khi nào dùng:** Thử nghiệm nhanh, máy không có Hadoop

**Lệnh:**
```bash
bash scripts/run_spark.sh "data/*videos.csv" \
  --no-sample \
  --num-trees 100 \
  --max-depth 12 \
  --save-metrics /tmp/rf_metrics.json
```

**Kiểm tra kết quả:**
```bash
cat /tmp/rf_metrics.json
```

---

### ✅ Cách 2: Chạy Spark + Lưu Visualizations

**Khi nào dùng:** Muốn xem biểu đồ model performance

**Lệnh:**
```bash
bash scripts/run_spark.sh "data/*videos.csv" \
  --no-sample \
  --num-trees 100 \
  --max-depth 12 \
  --save-visualizations Images
```

---

### ✅ Cách 3: Chạy Full HDFS + Spark

**Khi nào dùng:** Muốn chạy trên Hadoop HDFS, lưu model + predictions

**Bước 1: Upload dữ liệu từ Windows (nếu cần)**
```powershell
pwsh
.\app\scripts\scp_upload_raw.ps1 `
  -LocalCsv "C:\path\to\USvideos.csv" `
  -VmUser thinh `
  -VmHost 10.0.2.15
```

**Bước 2: Chạy orchestration script trên VM Linux**
```bash
cd /home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL\ PROJECT
sudo bash app/scripts/orchestrate_full_pipeline.sh \
  "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT/data/USvideos.csv" \
  --hdfs-user thinh \
  --num-trees 100 \
  --max-depth 12
```

---

### ✅ Cách 4: Jupyter Notebook

**Khi nào dùng:** Muốn xem từng bước, trực quan hóa dữ liệu

```bash
jupyter notebook "Predict YouTube Trending.ipynb"
```

---

## ⚙️ Unit Tests (Tùy chọn)

```bash
pip install pytest
python -m pytest app/tests/ -v
```

---

## 🐛 Khắc Phục Sự Cố

### Lỗi: `ModuleNotFoundError: No module named 'pyspark'`
```bash
pip install pyspark
```

### Lỗi: `No module named 'matplotlib'`
```bash
pip install matplotlib seaborn
```

### Lỗi: File CSV không tìm thấy
```bash
ls -lh data/*videos.csv
# hoặc
cp ~/Downloads/USvideos.csv ./data/
```

### Lỗi: `JAVA_HOME not set`
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

---

## 💡 Mẹo Nhanh

1. Chạy Cách 1 trước để kiểm tra pipeline local.
2. Dùng Cách 2 nếu cần biểu đồ.
3. Dùng Cách 3 khi cần lưu output lên HDFS.
4. Dùng Cách 4 để phân tích chi tiết trong notebook.

---

## 🔗 Liên Kết Nhanh

- README chi tiết: [README_YOUTUBE.md](README_YOUTUBE.md)
- HDFS setup: [app/scripts/README_HDFS_SETUP.md](app/scripts/README_HDFS_SETUP.md)
- Testing: [app/tests/README_TESTS.md](app/tests/README_TESTS.md)

---

**Bắt đầu nhanh:**
```bash
bash scripts/run_spark.sh "data/*videos.csv" --no-sample
```
