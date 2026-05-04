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
echo "  1. python3 run_demo.py          (Quick demo)"
echo "  2. python3 app/app.py           (CLI app)"
echo "  3. jupyter notebook             (Full pipeline)"
echo ""
