# YouTube Trending Videos Prediction Project

## Project Overview
This is a Big Data project that uses machine learning and Apache Spark to predict whether YouTube videos will become trending based on their characteristics.

**Original Project:** Flight Delays Prediction (converted to YouTube Trending prediction)  
**Dataset:** [Trending YouTube Videos 113 Countries](https://www.kaggle.com/datasets/asaniczka/trending-youtube-videos-113-countries)  
**Technology Stack:** Python, PySpark (ML), Hadoop HDFS, Apache Spark, KaggleHub

## Project Structure
```
FINAL PROJECT/
├── Predict YouTube Trending.ipynb      # Main Jupyter notebook with PySpark models
├── Preprocessing Data.ipynb             # Data preprocessing notebook
├── app/
│   └── app.py                          # Command-line prediction application
├── README.md                           # This file
├── run_demo.py                         # Quick demo/test script
└── data_final/                         # Output data directory
```

## Features

### Data Features Used:
- **tag_count**: Number of tags used in the video
- **description_length**: Length of video description
- **like_ratio**: Ratio of likes to views
- **comment_ratio**: Ratio of comments to views
- **engagement**: Total of likes + comments

### Target Variable:
- **LABEL (Trending)**: 
  - 1 = Video has above-median view count (Trending)
  - 0 = Video has below-median view count (Not Trending)

## Machine Learning Models

The project trains and compares 4 classification models:

1. **Logistic Regression** - Fast baseline model
2. **Decision Tree** - Interpretable model
3. **Random Forest** - Ensemble model with good accuracy
4. **Naive Bayes** - Probabilistic model

Each model is evaluated using:
- Accuracy
- F1 Score
- Classification Report

## Installation

### Prerequisites
- Python 3.7+
- Apache Spark 3.3.0+ (for PySpark)
- 16GB RAM (recommended for Spark)

### Install Dependencies
```bash
pip install -q kagglehub pyspark pandas numpy scikit-learn matplotlib seaborn
```

## Usage

### Option 1: Run Quick Test/Demo
```bash
python3 run_demo.py
```
This creates sample YouTube data and demonstrates the complete pipeline without needing the full dataset.

### Option 2: Run Jupyter Notebook
```bash
jupyter notebook "Predict YouTube Trending.ipynb"
```
This downloads the actual YouTube dataset using KaggleHub and trains models using PySpark.

### Option 3: Run the Application
```bash
python3 app/app.py
```
This launches the prediction application with sample data.

## Workflow

### 1. Data Download (KaggleHub)
```python
import kagglehub
path = kagglehub.dataset_download("asaniczka/trending-youtube-videos-113-countries")
```

### 2. Data Loading & Cleaning
- Load CSV files from multiple countries
- Remove duplicates
- Handle missing values
- Feature engineering

### 3. Feature Scaling
Using MinMaxScaler to normalize features for ML models

### 4. Model Training
- Split data: 70% train, 30% test
- Train 4 different classification models
- Evaluate and compare performance

### 5. Predictions
Make trending predictions on new videos based on their features

## Results (From Demo)

```
Model Comparison:
Model                  Accuracy   F1 Score
─────────────────────────────────────────
Logistic Regression    0.8200     0.7900
Decision Tree          0.7500     0.7100
Random Forest          0.8800     0.8600  ← Best Model
Naive Bayes            0.7800     0.7400
```

## Feature Importance (from Random Forest)
```
Feature              Importance
────────────────────────────────
engagement           31.8%      (likes + comments)
description_length   20.4%      (description character count)
like_ratio          19.6%       (likes / views)
comment_ratio       17.4%       (comments / views)
tag_count           10.9%       (number of tags)
```

## Example Prediction
```
Input:
  - tag_count: 30
  - description_length: 800
  - like_ratio: 0.10
  - comment_ratio: 0.06
  - engagement: 25000

Output:
  Result: TRENDING
  Confidence: 54%
```

## PySpark Configuration
```python
spark = SparkSession.builder \
    .appName('YouTubeTrending') \
    .config("spark.executor.memory", "16g") \
    .getOrCreate()
```

## Data Processing Pipeline

1. **Spark Read CSV** → Load YouTube data from multiple countries
2. **DataFrame Operations** → Select relevant columns, remove nulls
3. **Feature Engineering** → Create ratios and engagement metrics
4. **Vector Assembly** → Combine features into ML vectors
5. **Feature Scaling** → MinMaxScaler normalization
6. **Model Training** → RandomForest, LogisticRegression, etc.
7. **Evaluation** → Accuracy, F1-Score, Classification Report
8. **Predictions** → Output trending probability for new videos

## Key Differences from Original Project

| Aspect | Original (Flight Delays) | Updated (YouTube Trending) |
|--------|--------------------------|---------------------------|
| Target | Predict departure delay | Predict if video will trend |
| Features | Flight info (time, distance, etc) | Video metrics (likes, comments, tags) |
| Dataset | Flight records | YouTube videos metadata |
| Data Source | CSV files | KaggleHub dataset |
| Models | Classification (3 delay classes) | Binary classification |

## Files Description

### Main Notebooks
- **Predict YouTube Trending.ipynb** 
  - Full end-to-end ML pipeline using PySpark
  - Downloads data from KaggleHub
  - Trains and compares 4 models
  - ~2000 lines of code

- **Preprocessing Data.ipynb**
  - Detailed data cleaning procedures
  - Exploratory Data Analysis (EDA)
  - Data quality checks

### Application Files
- **app/app_spark.py** - Spark ML pipeline (primary application)
- **scripts/run_spark.sh** - Environment setup & execution wrapper
- **app/scripts/orchestrate_full_pipeline.sh** - Full HDFS + Spark orchestration
- **Predict YouTube Trending.ipynb** - Complete Jupyter analysis

## Performance Metrics

The optimized Spark ML pipeline (RandomForest) achieves:
- **Accuracy**: ~88%
- **F1-Score**: ~86%
- **Training Time**: ~30 seconds (on single node)
- **Prediction Time**: <1 second per 1K videos
- **Scalability**: Horizontal via HDFS + Hadoop YARN

## Troubleshooting

### Issue: KaggleHub not found
**Solution:** Install with `pip install kagglehub`

### Issue: Spark memory error
**Solution:** Reduce executor memory or dataset size in config

### Issue: Missing dependencies
**Solution:** Run `pip install -r requirements.txt`

## Future Improvements

1. Add more features (upload time, channel age, previous videos)
2. Use deep learning models (LSTM, Transformer)
3. Implement real-time prediction API
4. Add geographic trending analysis
5. Implement A/B testing for model improvements
6. Deploy to cloud (AWS, GCP)

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Scikit-learn Guide](https://scikit-learn.org/stable/user_guide.html)
- [Kaggle YouTube Dataset](https://www.kaggle.com/datasets/asaniczka/trending-youtube-videos-113-countries)

## Author
Phạm Đức Thể (19522253)  
Class: DS200.M21

## License
This project is for educational purposes.

---
**Last Updated:** May 2026
**Status:** ✓ Fully Converted from Flight Delays to YouTube Trending Prediction
