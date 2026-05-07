#!/usr/bin/env python3
"""Download the Kaggle YouTube trending dataset with kagglehub.

This script downloads the dataset and copies the relevant files into the
project's `data/` directory so the regression ETL can read `data/*videos.csv`.

Default dataset:
    datasnaek/youtube-new
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path


def _copy_selected_files(source_dir: Path, target_dir: Path) -> list[Path]:
    copied: list[Path] = []
    target_dir.mkdir(parents=True, exist_ok=True)

    for pattern in ("*videos.csv", "*category*.json"):
        for source_file in source_dir.glob(pattern):
            destination = target_dir / source_file.name
            shutil.copy2(source_file, destination)
            copied.append(destination)

    return copied


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Kaggle YouTube dataset via kagglehub")
    parser.add_argument(
        "--dataset",
        default="datasnaek/youtube-new",
        help="Kaggle dataset slug (default: datasnaek/youtube-new)",
    )
    parser.add_argument(
        "--target-dir",
        default=str(Path(__file__).resolve().parents[1] / "data"),
        help="Directory where CSV/JSON files will be copied (default: project data/)",
    )
    args = parser.parse_args()

    try:
        import kagglehub
    except ImportError:
        print("✗ Missing dependency: kagglehub. Install it in your venv first.")
        return 1

    dataset_path = Path(kagglehub.dataset_download(args.dataset)).resolve()
    target_dir = Path(args.target_dir).resolve()
    copied_files = _copy_selected_files(dataset_path, target_dir)

    print(f"✓ Kaggle dataset downloaded: {dataset_path}")
    print(f"✓ Copied {len(copied_files)} file(s) into: {target_dir}")
    for file_path in copied_files:
        print(f"  - {file_path.name}")

    if not copied_files:
        print("⚠ No matching files found. Check the dataset contents manually.")
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())