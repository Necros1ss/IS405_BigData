#!/usr/bin/env python3
"""Stream a CSV inside a zip archive and write ONLY APAC trending videos to output CSV.

Usage:
  python3 scripts/extract_apac.py \
    --archive /path/to/archive.zip \
    --out data/filtered_data.csv
"""

import argparse
import csv
import io
import sys
import zipfile


# =========================================================
# APAC COUNTRIES
# =========================================================
KEEP_COUNTRIES = {
    # Southeast Asia
    "VIETNAM", "THAILAND", "INDONESIA", "MALAYSIA", "PHILIPPINES", "SINGAPORE",
    # East Asia
    "JAPAN", "SOUTH KOREA", "HONG KONG", "TAIWAN",
    # South Asia
    "INDIA", "PAKISTAN", "BANGLADESH", "SRI LANKA", "NEPAL",
    # Oceania
    "AUSTRALIA", "NEW ZEALAND",
}

# =========================================================
# FIND COUNTRY COLUMN
# =========================================================
def find_country_column(fieldnames):
    country_keywords = ['video_trending_country', 'trending_country', 'country', 'region', 'location']
    for keyword in country_keywords:
        for fn in fieldnames:
            if keyword.lower() in fn.lower():
                return fn
    return None

# =========================================================
# FIND CSV INSIDE ZIP
# =========================================================
def find_csv_member(z: zipfile.ZipFile):
    for name in z.namelist():
        low = name.lower()
        if low.endswith('.csv') and 'youtube' in low:
            return name
    for name in z.namelist():
        if name.lower().endswith('.csv'):
            return name
    return None

# =========================================================
# MAIN
# =========================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--archive', required=True, help='Path to zip archive')
    p.add_argument('--out', required=True, help='Output CSV path')
    args = p.parse_args()

    archive = args.archive
    outpath = args.out

    total = 0
    kept = 0
    
    # Rổ chứa các quốc gia ĐÃ ĐƯỢC LƯU vào file mới
    saved_countries = set()

    # =====================================================
    # OPEN ZIP
    # =====================================================
    with zipfile.ZipFile(archive, 'r') as z:
        member = find_csv_member(z)

        if not member:
            print('No CSV found in archive', file=sys.stderr)
            sys.exit(2)

        print(f'Processing member: {member}')
        print('\nFiltering ONLY APAC trending videos...\n')

        # =================================================
        # OPEN CSV STREAM
        # =================================================
        with z.open(member, 'r') as raw:
            textf = io.TextIOWrapper(raw, encoding='utf-8', errors='replace')
            reader = csv.DictReader(textf)
            fieldnames = reader.fieldnames

            if not fieldnames:
                print('No header found in CSV', file=sys.stderr)
                sys.exit(2)

            # =============================================
            # FIND COUNTRY COLUMN
            # =============================================
            country_col = find_country_column(fieldnames)

            if not country_col:
                print('No country column found in CSV header', file=sys.stderr)
                sys.exit(2)

            print(f'Using country column: {country_col}\n')

            # =============================================
            # WRITE OUTPUT
            # =============================================
            with open(outpath, 'w', newline='', encoding='utf-8') as out_f:
                writer = csv.DictWriter(out_f, fieldnames=fieldnames)
                writer.writeheader()

                # =========================================
                # STREAM FILTERING
                # =========================================
                for row in reader:
                    total += 1
                    country = row.get(country_col, '').strip().upper()

                    # =====================================
                    # KEEP ONLY APAC COUNTRIES
                    # =====================================
                    if country in KEEP_COUNTRIES:
                        writer.writerow(row)
                        kept += 1
                        
                        # Chỉ add quốc gia vào rổ khi nó được giữ lại
                        saved_countries.add(country)

                    # Progress logging
                    if total % 100000 == 0:
                        print(f'Scanned {total:,} rows — kept {kept:,} APAC videos', flush=True)

    # =====================================================
    # DONE
    # =====================================================
    print('\n' + '=' * 70)
    print('DONE')
    print('=' * 70)

    print(f'✓ Total rows scanned : {total:,}')
    print(f'✓ APAC rows kept     : {kept:,}')

    # In ra danh sách thực tế đã được lưu
    print('\n✓ Countries successfully saved to output CSV:')
    for c in sorted(saved_countries):
        print(f'  - {c}')

    print(f'\n✓ Output saved to:')
    print(f'  {outpath}')
    print('=' * 70)

if __name__ == '__main__':
    main()