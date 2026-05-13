#!/usr/bin/env python3
"""
Tải dataset YouTube Trending từ Kaggle và lọc chỉ dữ liệu Việt Nam (VN).
Script này sẽ:
  1. Tải dataset canerkonuk/youtube-trending-videos-global
  2. Lọc chỉ các video trending ở Việt Nam
  3. Lưu vào file CSV riêng
  4. Xóa cache Kaggle để giải phóng ổ cứng
"""
import argparse
import csv
import os
import sys
import shutil
from pathlib import Path
from datetime import datetime

try:
    import kagglehub
    import pandas as pd
except ImportError:
    print("✗ Thiếu thư viện. Chạy lệnh:")
    print("  pip install kagglehub pandas")
    sys.exit(1)


def find_country_column(fieldnames):
    """Tìm cột chứa thông tin đất nước từ header"""
    country_keywords = ['country', 'trending_country', 'video_trending_country', 'region', 'location']
    
    for keyword in country_keywords:
        for fn in fieldnames:
            if keyword.lower() in fn.lower():
                return fn
    
    # Nếu không tìm được, trả về None
    return None


def find_title_column(fieldnames):
    """Tìm cột tiêu đề video"""
    title_keywords = ['title', 'video_title']
    
    for keyword in title_keywords:
        for fn in fieldnames:
            if keyword.lower() in fn.lower():
                return fn
    
    return fieldnames[0] if fieldnames else None


def main():
    parser = argparse.ArgumentParser(
        description='Tải YouTube Trending Videos từ Kaggle và lọc dữ liệu Việt Nam'
    )
    parser.add_argument(
        '--dataset', 
        default='canerkonuk/youtube-trending-videos-global',
        help='Dataset ID từ Kaggle (default: canerkonuk/youtube-trending-videos-global)'
    )
    parser.add_argument(
        '--out', 
        default='data/VN_youtube_trending.csv',
        help='Đường dẫn file CSV output (default: data/VN_youtube_trending.csv)'
    )
    parser.add_argument(
        '--country',
        default='VN',
        help='Mã đất nước cần lọc (default: VN)'
    )
    parser.add_argument(
        '--show-countries',
        action='store_true',
        help='Hiển thị tất cả các đất nước có trong dataset'
    )
    
    args = parser.parse_args()

    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("="*70)
    print("TẢI YOUTUBE TRENDING VIDEOS - LỌC DỮ LIỆU VIỆT NAM")
    print("="*70)
    print(f"Dataset: {args.dataset}")
    print(f"Lọc quốc gia: {args.country}")
    print(f"Output: {out_path}\n")

    # ===== BƯỚC 1: TẢI DATASET TỪ KAGGLE =====
    print("="*70)
    print("BƯỚC 1: ĐANG TẢI DATASET TỪ KAGGLE...")
    print("="*70)
    
    try:
        dataset_path = kagglehub.dataset_download(args.dataset)
        dataset_dir = Path(dataset_path)
        print(f"✓ Đã tải cache vào: {dataset_dir}\n")
    except Exception as e:
        print(f"✗ Lỗi khi tải dataset: {e}")
        sys.exit(1)

    # Tìm file CSV lớn nhất
    csv_files = list(dataset_dir.glob("*.csv"))
    if not csv_files:
        print("✗ Không tìm thấy file CSV nào trong dataset!")
        sys.exit(1)
    
    input_csv = max(csv_files, key=lambda p: p.stat().st_size)
    print(f"✓ Tìm thấy file: {input_csv.name}")
    print(f"  Kích thước: {input_csv.stat().st_size / 1024 / 1024:.2f} MB\n")

    # ===== BƯỚC 2: PHÂN TÍCH CỘT DỮ LIỆU =====
    print("="*70)
    print("BƯỚC 2: PHÂN TÍCH CẤU TRÚC DỮ LIỆU")
    print("="*70)
    
    try:
        df_sample = pd.read_csv(input_csv, nrows=5)
        print(f"✓ Tổng cột: {len(df_sample.columns)}")
        print(f"✓ Các cột trong dataset:")
        for i, col in enumerate(df_sample.columns, 1):
            print(f"  {i:2d}. {col}")
    except Exception as e:
        print(f"⚠ Cảnh báo: Không thể đọc sample ({e})")
        df_sample = None
    
    country_col = find_country_column(df_sample.columns) if df_sample is not None else None
    if country_col:
        print(f"\n✓ Tìm thấy cột đất nước: '{country_col}'")
    else:
        print(f"\n⚠ Cảnh báo: Không tìm thấy cột đất nước, sẽ cố gắng tìm...")

    title_col = find_title_column(df_sample.columns) if df_sample is not None else None
    print(f"✓ Cột tiêu đề: '{title_col}'\n")

    # ===== BƯỚC 3: STREAMING VÀ LỌC DỮ LIỆU =====
    print("="*70)
    print("BƯỚC 3: STREAMING DỮ LIỆU VÀ LỌC")
    print("="*70)

    total_rows = 0
    vietnam_rows = 0
    country_count = {}

    with open(input_csv, 'r', encoding='utf-8', errors='replace') as in_f:
        reader = csv.DictReader(in_f)
        fieldnames = reader.fieldnames
        
        if not fieldnames:
            print("✗ File CSV không có header!")
            sys.exit(1)

        # Nếu chưa tìm được cột, tìm từ header
        if not country_col:
            country_col = find_country_column(fieldnames)

        if not country_col:
            print(f"✗ Không tìm thấy cột đất nước trong file!")
            print(f"  Các cột có sẵn: {fieldnames}")
            sys.exit(1)

        print(f"✓ Sử dụng cột: '{country_col}'\n")

        # Nếu chỉ muốn xem các đất nước
        if args.show_countries:
            print("Đang quét dataset để liệt kê các đất nước...\n")
            for row in reader:
                country = row.get(country_col, '').strip().upper()
                if country:
                    country_count[country] = country_count.get(country, 0) + 1
                total_rows += 1
                
                if total_rows % 100000 == 0:
                    print(f"  Đã quét {total_rows:,} dòng...", flush=True)

            print(f"\n✓ Tổng cộng: {total_rows:,} dòng")
            print(f"\nCác quốc gia trong dataset:")
            for country in sorted(country_count.keys()):
                print(f"  {country:3s}: {country_count[country]:8,} video")
            
            # Không tạo file output, chỉ hiển thị thông tin
            return

        # Ngược lại, lọc và lưu file
        print(f"Đang lọc dữ liệu cho quốc gia: {args.country}\n")

        with open(out_path, 'w', newline='', encoding='utf-8') as out_f:
            writer = csv.DictWriter(out_f, fieldnames=fieldnames)
            writer.writeheader()

            # Reset file pointer
            in_f.seek(0)
            reader = csv.DictReader(in_f)

            for row in reader:
                total_rows += 1
                country = row.get(country_col, '').strip().upper()
                country_count[country] = country_count.get(country, 0) + 1

                # Lọc dữ liệu phù hợp
                if country == args.country.upper():
                    writer.writerow(row)
                    vietnam_rows += 1

                if total_rows % 100000 == 0:
                    print(f'  Quét {total_rows:,} dòng — Tìm thấy {vietnam_rows:,} video {args.country}', flush=True)

    # ===== BƯỚC 4: HIỂN THỊ KẾT QUẢ =====
    print("\n" + "="*70)
    print("BƯỚC 4: KẾT QUẢ FILTERING")
    print("="*70)
    print(f"✓ Tổng dòng đã quét: {total_rows:,}")
    print(f"✓ Dòng {args.country}: {vietnam_rows:,}")
    if total_rows > 0:
        percentage = (vietnam_rows / total_rows) * 100
        print(f"✓ Tỷ lệ: {percentage:.2f}%")

    if vietnam_rows > 0:
        print(f"\n✓ Đã lưu {vietnam_rows:,} video vào: {out_path}")
        
        # Hiển thị sample
        print(f"\n🔍 Sample 5 dòng đầu tiên:")
        df_result = pd.read_csv(out_path, nrows=5)
        print(df_result.to_string(index=False))
    else:
        print(f"\n⚠ Không tìm thấy video nào cho quốc gia: {args.country}")
        if country_count:
            print(f"\nCác quốc gia có sẵn:")
            for c in sorted(country_count.keys())[:10]:
                print(f"  - {c} ({country_count[c]} video)")

    # ===== BƯỚC 5: DỌN DẸP CACHE =====
    print("\n" + "="*70)
    print("BƯỚC 5: DỌN DẸP CACHE KAGGLE")
    print("="*70)
    
    try:
        shutil.rmtree(dataset_dir)
        print("✓ Đã xóa thành công cache Kaggle!")
        print("✓ Ổ cứng của bạn đã được giải phóng.\n")
    except Exception as e:
        print(f"⚠ Cảnh báo: Không thể xóa cache tự động ({e})")
        print(f"  Bạn có thể xóa tay thư mục: {dataset_dir}\n")

    print("="*70)
    print("✓ HOÀN TẤT!")
    print("="*70)


if __name__ == '__main__':
    main()
