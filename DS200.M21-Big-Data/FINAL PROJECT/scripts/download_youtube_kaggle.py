#!/usr/bin/env python3
"""
TẤT CẢ TRONG 1: Tải Kaggle -> Đọc luồng (Streaming) lọc ngày tháng -> Xóa Cache khổng lồ.
Tránh tình trạng bị Full Disk khi làm việc với Big Data.
"""
import argparse
import csv
import os
import re
import sys
import shutil
from pathlib import Path
from datetime import datetime

try:
    import kagglehub
except ImportError:
    print("✗ Thiếu thư viện: kagglehub. Chạy lệnh: pip install kagglehub")
    sys.exit(1)

def try_parse_date(s):
    if not s: return None
    s = s.strip()
    if not s: return None

    iso_fmts = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    for f in iso_fmts:
        try: return datetime.strptime(s, f)
        except ValueError: continue

    t = re.sub(r"[./]", "-", s)
    short_fmts = ["%y-%d-%m", "%y-%m-%d"]
    for f in short_fmts:
        try: return datetime.strptime(t, f)
        except ValueError: continue

    m = re.search(r"(20\d{2})", s)
    if m:
        try: return datetime(int(m.group(1)), 1, 1)
        except Exception: return None
            
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default='canerkonuk/youtube-trending-videos-global')
    # Lưu đúng tên file mà code Spark Clean của bạn đang chờ
    parser.add_argument('--out', default='data/GLOBAL_youtube_trending_2024_plus.csv') 
    # Cài đặt từ Tháng 5/2025 tới Tháng 4/2026
    parser.add_argument('--start-date', default='2025-05-01')
    parser.add_argument('--end-date', default='2026-04-30')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)

    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("="*60)
    print(f"1. ĐANG TẢI DATASET TỪ KAGGLE...")
    print("="*60)
    try:
        # kagglehub tự tải và giải nén file CSV vào ổ C (hoặc ~/.cache)
        dataset_path = kagglehub.dataset_download(args.dataset)
        dataset_dir = Path(dataset_path)
        print(f"✓ Đã tải cache vào: {dataset_dir}")
    except Exception as e:
        sys.exit(f"✗ Lỗi khi tải: {e}")

    # Tìm file CSV lớn nhất trong thư mục vừa tải
    csv_files = list(dataset_dir.glob("*.csv"))
    if not csv_files:
        sys.exit("✗ Không tìm thấy file CSV nào!")
    input_csv = max(csv_files, key=lambda p: p.stat().st_size)

    print("\n"+"="*60)
    print(f"2. BẮT ĐẦU STREAMING VÀ LỌC DỮ LIỆU")
    print("="*60)
    print(f"- File gốc: {input_csv.name}")
    print(f"- Lọc từ: {start_date.date()} đến {end_date.date()}")

    total, kept = 0, 0
    with open(input_csv, 'r', encoding='utf-8', errors='replace') as in_f, \
         open(out_path, 'w', newline='', encoding='utf-8') as out_f:

        reader = csv.DictReader(in_f)
        fieldnames = reader.fieldnames
        if not fieldnames: sys.exit("✗ File CSV không có header!")

        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()

        trending_col = next((fn for fn in fieldnames if 'trend' in fn.lower()), None)
        publish_col = next((fn for fn in fieldnames if 'publish' in fn.lower()), None)

        for row in reader:
            total += 1
            date_val = row.get(publish_col) if publish_col else None
            if not date_val and trending_col: date_val = row.get(trending_col)
                
            if not date_val:
                for v in row.values():
                    if v and re.search(r"20\d{2}", v):
                        date_val = v
                        break

            dt = try_parse_date(date_val)
            
            # Chỉ ghi những dòng nằm trong năm 2025-2026
            if dt and start_date <= dt <= end_date:
                writer.writerow(row)
                kept += 1

            if total % 200000 == 0:
                print(f'   Quét {total:,} dòng — Giữ lại {kept:,} dòng', flush=True)

    print(f"✓ Lọc hoàn tất! Đã lưu file sạch ({kept:,} dòng) tại: {out_path}")

    print("\n"+"="*60)
    print("3. DỌN DẸP CACHE GIẢI PHÓNG Ổ CỨNG")
    print("="*60)
    try:
        shutil.rmtree(dataset_dir)
        print("✓ Đã xóa thành công thư mục Kaggle Cache khổng lồ!")
        print("✓ Ổ cứng của bạn đã được giải phóng.")
    except Exception as e:
        print(f"⚠ Lỗi xóa thư mục (bạn có thể xóa tay): {e}")

if __name__ == '__main__':
    main()