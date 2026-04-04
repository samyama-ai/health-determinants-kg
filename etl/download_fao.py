"""Download water/sanitation indicators from WHO GHO (AQUASTAT alternative).

The FAO AQUASTAT portal requires manual download, so we use equivalent
WHO GHO indicators for safely managed/basic water and sanitation.

Usage:
    python -m etl.download_fao --data-dir data
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import requests

GHO_BASE = "https://ghoapi.azureedge.net/api"

WATER_INDICATORS = {
    "WSH_SANITATION_SAFELY_MANAGED": "safely_managed_sanitation",
    "WSH_WATER_SAFELY_MANAGED": "safely_managed_water",
    "WSH_SANITATION_BASIC": "basic_sanitation",
    "WSH_WATER_BASIC": "basic_water",
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


def download_aquastat(data_dir: str | Path) -> dict:
    """Download water/sanitation indicators from WHO GHO."""
    out_path = Path(data_dir) / "fao"
    out_path.mkdir(parents=True, exist_ok=True)

    print("Downloading Water & Sanitation indicators from WHO GHO...")
    rows = []
    for code, name in WATER_INDICATORS.items():
        records = _fetch_gho_all(code)
        count = 0
        for r in records:
            cc = str(r.get("SpatialDim", "")).strip()
            year = str(r.get("TimeDim", "")).strip()
            value = r.get("NumericValue")
            if cc and year and value is not None and len(cc) == 3:
                rows.append({
                    "country_code": cc,
                    "indicator_code": name,
                    "indicator_name": code,
                    "year": year,
                    "value": round(value, 2),
                })
                count += 1
        print(f"  {code} ({name}): {count} records")
        time.sleep(0.3)

    csv_path = out_path / "aquastat.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["country_code", "indicator_code", "indicator_name", "year", "value"])
        w.writeheader()
        w.writerows(rows)
    print(f"  Total: {len(rows)} records → {csv_path}")
    return {"records": len(rows)}


def main():
    parser = argparse.ArgumentParser(description="Download water/sanitation data")
    parser.add_argument("--data-dir", default="data", help="Output directory")
    args = parser.parse_args()
    download_aquastat(args.data_dir)


if __name__ == "__main__":
    main()
