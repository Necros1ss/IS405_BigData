# YouTube Trending Videos Prediction - QUICKSTART

## 🚀 Get Started

### ⭐ Recommended: HDFS + Spark Pipeline (Big Data Flow)

This workflow mimics real-world Big Data architecture: Windows → HDFS → Spark → Model & Predictions.

**On Windows:**
```powershell
# Download raw CSV from Kaggle (no cleaning needed)
# Then upload to VM:
pwsh .\app\scripts\scp_upload_raw.ps1 -LocalCsv "C:\path\to\USvideos.csv" -VmUser thinh -VmHost 10.0.2.15
```

**On VM (Lubuntu):**
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
# Full orchestration: cài Hadoop (nếu cần) → put CSV → chạy Spark
sudo bash app/scripts/orchestrate_full_pipeline.sh "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT/data/USvideos.csv" --hdfs-user thinh --num-trees 100 --max-depth 12
```

**Outputs saved to:**
- Model: `/user/thinh/models/rf/` (HDFS)
- Predictions: `/user/thinh/predictions/rf/` (HDFS + optional CSV)
- Metrics: `/tmp/rf_metrics.json` (local)

---

### Option 1: Quick Spark Run (Local Data)

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH
export SPARK_HOME=/home/thinh/spark
export SPARK_LOCAL_HOSTNAME=localhost
export PYTHONPATH=/home/thinh/spark/python:/home/thinh/spark/python/lib/pyspark.zip:/home/thinh/spark/python/lib/py4j-0.10.9.9-src.zip

cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
source .venv_spark/bin/activate
python3 app/app_spark.py --data "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT/data/*videos.csv"
```

Notes:
- Uses the local Spark install at `/home/thinh/spark` instead of pip `pyspark`.
- Uses Java 17. Java 25 EA caused Spark errors on this machine.
- The default local dataset pattern is `data/*videos.csv`.
- The training step is intentionally sampled so the job can finish on this VM.

### Running heavier / full training
If you have enough resources and want to train with larger settings (or on full split), use these flags:

```bash
# sample fraction controls fraction after train/test split; set --no-sample to train on full split
python3 app/app_spark.py --data "data/*videos.csv" --sample-fraction 0.1 --num-trees 50 --max-depth 10

# or to train on the full split (be careful: expensive)
python3 app/app_spark.py --data "data/*videos.csv" --no-sample --num-trees 100 --max-depth 12
```

Use `scripts/run_spark.sh` to wrap environment exports:

```bash
bash scripts/run_spark.sh "data/*videos.csv" --no-sample --num-trees 100 --max-depth 12
```

### Option 2: Full Jupyter Pipeline (Advanced)
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
jupyter notebook "Predict YouTube Trending.ipynb"
```
**Features:**
- Downloads YouTube dataset from Kaggle (10GB+)
- Uses Apache Spark for distributed computing
- Trains 4 ML models
- Detailed analysis & visualizations

---

## 📋 What You Get

### Quick Results (Demo & CLI)
- ✅ YouTube video trending predictions
- ✅ Feature importance analysis
- ✅ Model accuracy metrics
- ✅ Sample data processing

### Full Results (Jupyter Notebook)
- ✅ Real YouTube data (113 countries)
- ✅ Apache Spark ML pipeline
- ✅ 4 classification models
- ✅ Comprehensive EDA
- ✅ Model comparison & evaluation

---

## 📊 Expected Output

### Demo Output
```
Training accuracy: 100.00%

Predictions:
  Video 1: TRENDING        (Confidence: 100%)
  Video 2: NOT TRENDING    (Confidence: 100%)
  Video 3: NOT TRENDING    (Confidence: 66%)
  Video 4: NOT TRENDING    (Confidence: 96%)

Feature Importance:
  description_length  26%
  engagement          26%
  comment_ratio       18%
  like_ratio          16%
  tag_count           14%
```

### Model Performance
- **Accuracy:** ~88%
- **Precision:** ~85%
- **Recall:** ~87%
- **F1-Score:** ~86%

---

## 🔧 Troubleshooting

### Import Error: "No module named 'pyspark'"
**Solution:** Only needed for Jupyter notebook
```bash
pip install pyspark
```

### Import Error: "No module named 'kagglehub'"
**Solution:** Only needed for real dataset download
```bash
pip install kagglehub
```

### Permission Denied on setup.sh
```bash
chmod +x setup.sh
./setup.sh
```

---

## 📁 Project Structure

```
FINAL PROJECT/
├── Predict YouTube Trending.ipynb    ← Jupyter (Full Pipeline)
├── app/
│   └── app.py                        ← CLI Application
├── run_demo.py                       ← Quick Demo
├── setup.sh                          ← Setup Script
├── requirements.txt                  ← Dependencies
└── README_YOUTUBE.md                 ← Full Documentation
```

---

## ⚡ Pro Tips

1. **First time?** → Start with `python3 run_demo.py`
2. **Want real data?** → Install PySpark and run Jupyter
3. **Need predictions?** → Use `python3 app/app.py`
4. **Testing changes?** → Edit files and re-run

---

## ✅ Verification Checklist

Run this to verify project is working:
```bash
python3 -c "
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
print('✓ All required packages available!')
"
```

Expected: `✓ All required packages available!`

---

## 📖 Learn More

- **Full docs:** See `README_YOUTUBE.md`
- **Original project:** `Predict Flight Delays.ipynb`
- **Dataset info:** https://www.kaggle.com/datasets/asaniczka/trending-youtube-videos-113-countries

---

**Questions?** Check the documentation files or run the demo!
