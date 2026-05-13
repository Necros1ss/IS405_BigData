#!/bin/bash
# YouTube Trending Videos Prediction - Setup Script
# This script sets up the entire project environment

echo "==============================================="
echo "YouTube Trending Prediction - Setup"
echo "==============================================="
echo ""

# Check Python version
echo "▶ Checking Python version..."
python3 --version

# Create virtual environment (optional)
echo ""
echo "▶ Setting up environment..."

# Try to install with --break-system-packages if needed
echo "  Installing dependencies..."
pip install -q numpy pandas scikit-learn matplotlib seaborn

# Try PySpark and KaggleHub (may fail in restricted environments)
echo "  Installing Big Data frameworks..."
pip install -q pyspark kagglehub 2>/dev/null || echo "  ⚠️  PySpark/KaggleHub installation skipped (may need manual install)"

echo ""
echo "▶ Verifying imports..."
python3 << PYTHON_CHECK
import sys
packages = {
    'numpy': 'NumPy',
    'pandas': 'Pandas', 
    'sklearn': 'Scikit-learn',
    'matplotlib': 'Matplotlib',
}

for pkg, name in packages.items():
    try:
        __import__(pkg)
        print(f"  ✓ {name}")
    except ImportError:
        print(f"  ✗ {name}")

try:
    import pyspark
    print(f"  ✓ PySpark")
except ImportError:
    print(f"  ⚠️  PySpark (optional)")

try:
    import kagglehub
    print(f"  ✓ KaggleHub")
except ImportError:
    print(f"  ⚠️  KaggleHub (optional)")
PYTHON_CHECK

echo ""
echo "==============================================="
echo "Setup Complete!"
echo "==============================================="
echo ""
echo "Ready to run:"
echo "  1. python3 -m app.app_spark_v2_fixed          (Quick batch run)"
echo "  2. python3 -m app.train_spark_v2_fixed        (Train model)"
echo "  3. python3 -m app.streaming_spark            (Run streaming prediction)"
echo "  4. jupyter notebook                           (Full pipeline notebook)"
echo ""
