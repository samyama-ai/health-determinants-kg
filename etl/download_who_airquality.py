"""Download WHO Air Quality (PM2.5) data from GHO API.

Usage:
    python -m etl.download_who_airquality --data-dir data
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import requests

GHO_BASE = "https://ghoapi.azureedge.net/api"

AIR_QUALITY_INDICATORS = {
    "AIR_41": "PM2.5 concentrations",
}


def _fetch_gho_all(code: str) -> list[dict]:
    """Fetch all records from WHO GHO OData endpoint with pagination."""
    records = []
    url = f"{GHO_BASE}/{code}"
    while url:
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            records.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
        except Exception as e:
            print(f"    [ERROR] {url}: {e}")
            break
    return records


def download_air_quality(data_dir: str | Path) -> dict:
    """Download WHO air quality (PM2.5) data from GHO."""
    out_path = Path(data_dir) / "who_airquality"
    out_path.mkdir(parents=True, exist_ok=True)

    print("Downloading WHO Air Quality (PM2.5) from GHO...")
    rows = []
    for code, name in AIR_QUALITY_INDICATORS.items():
        records = _fetch_gho_all(code)
        count = 0
        for r in records:
            cc = str(r.get("SpatialDim", "")).strip()
            year = str(r.get("TimeDim", "")).strip()
            value = r.get("NumericValue")
            if cc and year and value is not None and len(cc) == 3:
                rows.append({
                    "country_code": cc,
                    "city": cc,
                    "year": year,
                    "pm25": round(value, 2),
                    "pm10": "",
                })
                count += 1
        print(f"  {code} ({name}): {count} records")
        time.sleep(0.3)

    # Deduplicate by country+year
    seen = set()
    deduped = []
    for r in rows:
        key = f"{r['country_code']}-{r['year']}"
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    csv_path = out_path / "air_quality.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["country_code", "city", "year", "pm25", "pm10"])
        w.writeheader()
        w.writerows(deduped)
    print(f"  Total: {len(deduped)} records (deduped from {len(rows)}) → {csv_path}")
    return {"records": len(deduped)}


def main():
    parser = argparse.ArgumentParser(description="Download WHO Air Quality data")
    parser.add_argument("--data-dir", default="data", help="Output directory")
    args = parser.parse_args()
    download_air_quality(args.data_dir)


if __name__ == "__main__":
    main()
