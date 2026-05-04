# Testing Guide

## Running Unit Tests

### Prerequisites
```bash
pip install pytest pyspark matplotlib
```

### Run All Tests
```bash
cd "/home/thinh/Documents/IS_BigData/DS200.M21-Big-Data/FINAL PROJECT"
python -m pytest app/tests/ -v
```

### Run Specific Test File
```bash
# Test data cleaning and feature engineering
python -m pytest app/tests/test_clean_spark.py -v

# Test model training
python -m pytest app/tests/test_train_spark.py -v
```

### Run Specific Test Case
```bash
python -m pytest app/tests/test_clean_spark.py::TestCleanSpark::test_engineer_features_creates_columns -v
```

### Test with Coverage Report
```bash
pip install pytest-cov
python -m pytest app/tests/ --cov=app --cov-report=html
# Open htmlcov/index.html in browser
```

---

## What Each Test Module Tests

### `test_clean_spark.py`
- `test_normalize_input_columns_rename()` - Column renaming
- `test_normalize_input_columns_add_missing()` - Adding missing columns
- `test_engineer_features_creates_columns()` - Feature creation
- `test_engineer_features_calculates_ratios()` - Ratio calculations
- `test_engineer_features_tag_count()` - Tag count extraction
- `test_engineer_features_description_length()` - Description length calculation
- `test_engineer_features_drops_nulls()` - Null row removal
- `test_load_csv_with_spark()` - CSV loading

### `test_train_spark.py`
- `test_train_spark_model_returns_tuple()` - Model output format
- `test_model_has_auc_metric()` - AUC metric calculation
- `test_model_has_feature_importances()` - Feature importance extraction
- `test_predictions_have_label_column()` - Prediction output format
- `test_sampling_reduces_rows()` - Sampling functionality
- `test_different_hyperparameters()` - Hyperparameter effects

---

## Running Full Pipeline with Visualization

### Quick Test (local data, small sample)
```bash
bash scripts/run_spark.sh "kaggle_youtube/trending_yt_videos_113_countries.csv" \
  --no-sample \
  --num-trees 10 \
  --max-depth 6 \
  --save-metrics /tmp/rf_metrics.json \
  --save-visualizations Images
```

### Outputs
After running, check:
- **Model metrics:** `/tmp/rf_metrics.json`
- **Visualizations:** 
  - `Images/feature_importance.png` - Bar chart of feature importances
  - `Images/model_metrics.png` - Model performance summary
  - `Images/data_engagement_dist.png` - Distribution of engagement metric
  - `Images/data_like_ratio_dist.png` - Distribution of like ratio

### Disable Visualizations
```bash
bash scripts/run_spark.sh "your_data.csv" --no-visualizations
```

---

## Testing Kafka Streaming (Future)

### Preview
```bash
# View scaffold (no-op, shows structure)
python -m app.streaming_spark --help
```

**Note:** Full Kafka streaming requires:
- Kafka broker running on `localhost:9092`
- Pre-trained model saved at HDFS path
- Input topic with properly formatted JSON messages

See `app/streaming_spark.py` for integration details.

---

## CI/CD Integration

### Example GitHub Actions Workflow
```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - run: pip install -r requirements.txt pytest
      - run: python -m pytest app/tests/ -v
```

---

## Troubleshooting

**Issue:** `No module named 'pyspark'`
```bash
pip install pyspark
```

**Issue:** `No module named 'matplotlib'`
```bash
pip install matplotlib
```

**Issue:** Tests run but visualizations fail
- Visualization failures are non-critical
- Check if `Images/` directory exists: `mkdir -p Images`
- Run with `--no-visualizations` flag if issues persist

---
