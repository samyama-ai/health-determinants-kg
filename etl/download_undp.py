"""Download UNDP Human Development Index (HDI) data.

Tries the UNDP HDR GitHub data repository first, falls back to manual instructions.

Usage:
    python -m etl.download_undp --data-dir data
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import requests

HDI_GITHUB_URL = "https://raw.githubusercontent.com/UNDP-Data/HDR-Data/main/data/hdi.csv"


def download_hdi(data_dir: str | Path) -> dict:
    """Download UNDP HDI data from HDR GitHub data repo."""
    out_path = Path(data_dir) / "undp"
    out_path.mkdir(parents=True, exist_ok=True)
    csv_path = out_path / "hdi.csv"

    # Check for existing data
    if csv_path.exists():
        with open(csv_path) as f:
            count = sum(1 for _ in csv.DictReader(f))
        print(f"  Found existing {csv_path}: {count} records")
        return {"records": count}

    # Try GitHub
    print("Downloading UNDP HDI from GitHub HDR-Data...")
    try:
        r = requests.get(HDI_GITHUB_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"  ERROR: {e}")
        print(f"  Manual download from: https://hdr.undp.org/data-center/")
        print(f"  Place at: {csv_path}")
        print("  Expected columns: country_code, year, hdi, rank")
        return {"records": 0}

    lines = r.text.strip().split("\n")
    reader = csv.DictReader(lines)

    hdi_rows = []
    for row in reader:
        cc = row.get("iso3", "").strip()
        if not cc:
            continue
        for col_name, col_val in row.items():
            if col_name.isdigit() and col_val and col_val.strip() != "..":
                try:
                    hdi_rows.append({
                        "country_code": cc,
                        "year": int(col_name),
                        "hdi": round(float(col_val), 4),
                        "rank": "",
                    })
                except ValueError:
                    pass

    if hdi_rows:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["country_code", "year", "hdi", "rank"])
            w.writeheader()
            w.writerows(hdi_rows)
        print(f"  {len(hdi_rows)} HDI records → {csv_path}")
    else:
        print("  No valid records. Download manually from: https://hdr.undp.org/data-center/")
        print(f"  Place at: {csv_path}")
    return {"records": len(hdi_rows)}


def main():
    parser = argparse.ArgumentParser(description="Download UNDP HDI data")
    parser.add_argument("--data-dir", default="data", help="Output directory")
    args = parser.parse_args()
    download_hdi(args.data_dir)


if __name__ == "__main__":
    main()
