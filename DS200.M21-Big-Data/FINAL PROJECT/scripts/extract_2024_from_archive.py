#!/usr/bin/env python3
"""Stream a CSV inside a zip archive and write rows within a specific date range to output CSV.

Usage:
  python3 scripts/extract_2024_from_archive.py --archive /path/to/archive.zip --out data/filtered_data.csv
"""
import argparse
import csv
import io
import re
import sys
import zipfile
from datetime import datetime

try:
    from dateutil import parser as dateutil_parser
except Exception:
    dateutil_parser = None


def try_parse_date(s):
    if not s:
        return None
    s = s.strip()
    if not s:
        return None

    # 1. Ưu tiên cao nhất: Bắt chính xác định dạng ISO 8601 của video_published_at
    iso_fmts = [
        "%Y-%m-%dT%H:%M:%SZ",       # Ví dụ: 2024-10-11T00:00:06Z
        "%Y-%m-%dT%H:%M:%S.%fZ",    # Dự phòng nếu có mili-giây
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]
    for f in iso_fmts:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue

    # 2. Xử lý định dạng "dị" của trending_date (YY.DD.MM hoặc YY.MM.DD)
    t = re.sub(r"[./]", "-", s)
    short_fmts = [
        "%y-%d-%m", # Năm 2 chữ số - Ngày - Tháng (24-15-01)
        "%y-%m-%d"  # Năm 2 chữ số - Tháng - Ngày (24-01-15)
    ]
    for f in short_fmts:
        try:
            return datetime.strptime(t, f)
        except ValueError:
            continue

    # 3. Fallback sang dateutil (nếu có cài đặt)
    if dateutil_parser:
        try:
            return dateutil_parser.parse(s, fuzzy=True)
        except Exception:
            pass

    # 4. Giải pháp cuối cùng: Dùng Regex tìm năm 4 chữ số
    m = re.search(r"(20\d{2})", s)
    if m:
        try:
            return datetime(int(m.group(1)), 1, 1)
        except Exception:
            return None
            
    return None


def find_csv_member(z: zipfile.ZipFile):
    for name in z.namelist():
        if name.lower().endswith('.csv') and 'youtube' in name.lower():
            return name
    for name in z.namelist():
        if name.lower().endswith('.csv'):
            return name
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--archive', required=True, help='Path to zip archive')
    p.add_argument('--out', required=True, help='Output CSV path')
    
    # Thiết lập ngày bắt đầu và kết thúc (Mặc định: 01/01/2025 -> 31/03/2026)
    p.add_argument('--start-date', type=str, default='2025-01-01', help='Start date (Format: YYYY-MM-DD)')
    p.add_argument('--end-date', type=str, default='2026-03-31', help='End date (Format: YYYY-MM-DD)')
    args = p.parse_args()

    archive = args.archive
    outpath = args.out
    
    # Parse chuỗi ngày truyền vào thành object datetime
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        # Ép thời gian của end_date về cuối ngày để bao gồm trọn vẹn ngày đó
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    except ValueError:
        print("Lỗi: --start-date và --end-date phải theo định dạng YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    total = 0
    kept = 0

    with zipfile.ZipFile(archive, 'r') as z:
        member = find_csv_member(z)
        if not member:
            print('No CSV found in archive', file=sys.stderr)
            sys.exit(2)
        print(f'Processing member: {member}')
        print(f'Filtering records from {start_date} to {end_date}')
        
        with z.open(member, 'r') as raw:
            textf = io.TextIOWrapper(raw, encoding='utf-8', errors='replace')
            reader = csv.DictReader(textf)
            fieldnames = reader.fieldnames
            if not fieldnames:
                print('No header found in CSV', file=sys.stderr)
                sys.exit(2)

            trending_col = None
            publish_col = None
            for fn in fieldnames:
                low = fn.lower()
                if 'trend' in low:
                    trending_col = fn
                if 'publish' in low and publish_col is None:
                    publish_col = fn

            if not trending_col and not publish_col:
                print('No trending or publish column found in CSV header; will attempt year search in any column', file=sys.stderr)

            with open(outpath, 'w', newline='', encoding='utf-8') as out_f:
                writer = csv.DictWriter(out_f, fieldnames=fieldnames)
                writer.writeheader()

                for row in reader:
                    total += 1
                    date_val = None
                    
                    if publish_col:
                        date_val = row.get(publish_col)
                    if not date_val and trending_col:
                        date_val = row.get(trending_col)
                        
                    if not date_val:
                        for k, v in row.items():
                            if v and re.search(r"20\d{2}", v):
                                date_val = v
                                break

                    dt = try_parse_date(date_val)
                    
                    # Logic kiểm tra khoảng thời gian (Between start_date and end_date)
                    if dt and start_date <= dt <= end_date:
                        writer.writerow(row)
                        kept += 1

                    if total % 100000 == 0:
                        print(f'Scanned {total:,} rows — kept {kept:,}', flush=True)

    print(f'Done. Scanned {total:,} rows, kept {kept:,} rows. Output: {outpath}')

if __name__ == '__main__':
    main()