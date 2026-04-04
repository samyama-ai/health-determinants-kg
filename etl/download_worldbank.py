"""Download World Bank WDI (World Development Indicators) data.

Fetches curated indicators across 5 categories: socioeconomic,
environmental, nutrition, demographic, water — for all countries.

Usage:
    python -m etl.download_worldbank --data-dir data
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import requests

WB_BASE = "https://api.worldbank.org/v2"

# --- Curated indicators by category (~80 total) ---

SOCIOECONOMIC_INDICATORS = {
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "NY.GDP.PCAP.PP.CD": "GDP per capita, PPP (current international $)",
    "NY.GNP.PCAP.CD": "GNI per capita (current US$)",
    "SI.POV.DDAY": "Poverty headcount ratio at $2.15/day (%)",
    "SI.POV.NAHC": "Poverty headcount ratio at national poverty lines (%)",
    "SI.POV.GINI": "Gini index",
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",
    "SL.UEM.1524.ZS": "Youth unemployment (% ages 15-24)",
    "SE.ADT.LITR.ZS": "Literacy rate, adult total (%)",
    "SE.PRM.CMPT.ZS": "Primary completion rate (%)",
    "SE.SEC.ENRR": "School enrollment, secondary (% gross)",
    "SE.XPD.TOTL.GD.ZS": "Govt expenditure on education (% of GDP)",
    "SH.XPD.CHEX.GD.ZS": "Current health expenditure (% of GDP)",
    "SH.XPD.CHEX.PC.CD": "Current health expenditure per capita (US$)",
    "IC.BUS.EASE.XQ": "Ease of doing business score",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
}

ENVIRONMENTAL_INDICATORS = {
    "EN.ATM.PM25.MC.M3": "PM2.5 air pollution, mean annual exposure (ug/m3)",
    "EN.ATM.PM25.MC.ZS": "PM2.5 air pollution, pop exposed to unsafe levels (%)",
    "EN.ATM.CO2.PC": "CO2 emissions (metric tons per capita)",
    "EN.ATM.CO2.KT": "CO2 emissions (kt)",
    "AG.LND.FRST.ZS": "Forest area (% of land area)",
    "EG.FEC.RNEW.ZS": "Renewable energy consumption (% of total)",
    "EN.CLC.MDAT.ZS": "Droughts, floods, extreme temperatures (% pop affected)",
    "EG.USE.PCAP.KG.OE": "Energy use (kg of oil equivalent per capita)",
    "EN.ATM.GHGT.KT.CE": "Total greenhouse gas emissions (kt CO2 equivalent)",
    "AG.LND.ARBL.ZS": "Arable land (% of land area)",
}

NUTRITION_INDICATORS = {
    "SH.STA.STNT.ZS": "Stunting prevalence, height for age (% under 5)",
    "SH.STA.WAST.ZS": "Wasting prevalence, weight for height (% under 5)",
    "SH.STA.OWGH.ZS": "Overweight prevalence, weight for height (% under 5)",
    "SH.STA.MALN.ZS": "Malnutrition prevalence, weight for age (% under 5)",
    "SN.ITK.DEFC.ZS": "Prevalence of undernourishment (% of population)",
    "SH.STA.ANEM.ZS": "Prevalence of anemia, women of reproductive age (%)",
    "SH.STA.BRTW.ZS": "Low-birthweight babies (% of births)",
    "SH.STA.BFED.ZS": "Exclusive breastfeeding (% of infants under 6 months)",
    "SH.SVR.WAST.ZS": "Severe wasting prevalence (% under 5)",
    "SN.ITK.VITA.ZS": "Vitamin A supplementation coverage rate (%)",
}

DEMOGRAPHIC_INDICATORS = {
    "SP.POP.TOTL": "Population, total",
    "SP.POP.GROW": "Population growth (annual %)",
    "SP.DYN.LE00.IN": "Life expectancy at birth, total (years)",
    "SP.DYN.TFRT.IN": "Fertility rate, total (births per woman)",
    "SP.DYN.CBRT.IN": "Birth rate, crude (per 1,000 people)",
    "SP.DYN.CDRT.IN": "Death rate, crude (per 1,000 people)",
    "SP.DYN.IMRT.IN": "Mortality rate, infant (per 1,000 live births)",
    "SH.DYN.MORT": "Under-5 mortality rate (per 1,000 live births)",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    "SP.POP.DPND": "Age dependency ratio (% of working-age)",
    "SP.POP.65UP.TO.ZS": "Population ages 65+ (% of total)",
    "SM.POP.NETM": "Net migration",
    "SH.DYN.NMRT": "Neonatal mortality rate (per 1,000 live births)",
    "SP.DYN.AMRT.MA": "Mortality rate, adult, male (per 1,000)",
    "SP.DYN.AMRT.FE": "Mortality rate, adult, female (per 1,000)",
}

WATER_INDICATORS = {
    "SH.H2O.SMDW.ZS": "Safely managed drinking water services (% pop)",
    "SH.H2O.BASW.ZS": "At least basic drinking water services (% pop)",
    "SH.STA.SMSS.ZS": "Safely managed sanitation services (% pop)",
    "SH.STA.BASS.ZS": "At least basic sanitation services (% pop)",
    "SH.STA.HYGN.ZS": "Handwashing facility with soap and water (%)",
    "ER.H2O.FWTL.ZS": "Annual freshwater withdrawals, total (% of internal resources)",
    "ER.H2O.FWAG.ZS": "Annual freshwater withdrawals, agriculture (% of total)",
    "ER.H2O.FWIN.ZS": "Annual freshwater withdrawals, industry (% of total)",
    "ER.H2O.FWDM.ZS": "Annual freshwater withdrawals, domestic (% of total)",
    "SH.STA.WASH.P5": "Mortality attributed to unsafe WASH (per 100,000)",
}

ALL_CATEGORIES = {
    "socioeconomic": SOCIOECONOMIC_INDICATORS,
    "environmental": ENVIRONMENTAL_INDICATORS,
    "nutrition": NUTRITION_INDICATORS,
    "demographic": DEMOGRAPHIC_INDICATORS,
    "water": WATER_INDICATORS,
}


def _fetch_wb_json(url: str, params: dict | None = None, retries: int = 3) -> list:
    """Fetch JSON from World Bank API with retries and pagination."""
    if params is None:
        params = {}
    params.setdefault("format", "json")
    params.setdefault("per_page", "1000")

    all_records = []
    page = 1

    while True:
        params["page"] = str(page)
        for attempt in range(retries):
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(1)
                else:
                    print(f"  [ERROR] {url}: {e}")
                    return all_records

        if not isinstance(data, list) or len(data) < 2:
            break

        meta = data[0]
        records = data[1] if data[1] else []
        all_records.extend(records)

        total_pages = meta.get("pages", 1)
        if page >= total_pages:
            break
        page += 1

    return all_records


def download_countries(data_dir: Path) -> list:
    """Download country list from World Bank."""
    print("Downloading countries...")
    records = _fetch_wb_json(f"{WB_BASE}/country", {"per_page": "300"})
    # Filter to actual countries (not aggregates)
    countries = [
        r for r in records
        if r.get("region", {}).get("id", "") != "NA"
        and r.get("capitalCity", "") != ""
    ]
    out_path = data_dir / "countries.json"
    with open(out_path, "w") as f:
        json.dump(countries, f, indent=2)
    print(f"  {len(countries)} countries")
    return countries


def download_regions(data_dir: Path, countries: list) -> list:
    """Extract unique regions from country data."""
    print("Extracting regions...")
    seen = set()
    regions = []
    for c in countries:
        region = c.get("region", {})
        code = region.get("id", "")
        name = region.get("value", "")
        if code and code not in seen:
            seen.add(code)
            regions.append({"code": code, "name": name})
    out_path = data_dir / "regions.json"
    with open(out_path, "w") as f:
        json.dump(regions, f, indent=2)
    print(f"  {len(regions)} regions")
    return regions


def download_indicator_category(
    data_dir: Path,
    category: str,
    indicators: dict[str, str],
) -> int:
    """Download all indicators in a category for all countries."""
    print(f"Downloading {category} ({len(indicators)} indicators)...")
    all_records = []

    for code, name in indicators.items():
        records = _fetch_wb_json(
            f"{WB_BASE}/country/all/indicator/{code}",
            {"date": "1990:2024", "per_page": "1000"},
        )
        # Filter out null values
        valid = [r for r in records if r.get("value") is not None]
        all_records.extend(valid)
        print(f"  {code}: {len(valid)} records")
        time.sleep(0.5)  # Rate limit

    out_path = data_dir / f"{category}.json"
    with open(out_path, "w") as f:
        json.dump(all_records, f)
    print(f"  {category} total: {len(all_records)} records")
    return len(all_records)


def download_all(data_dir: str | Path) -> dict:
    """Download all World Bank WDI data."""
    data_path = Path(data_dir) / "worldbank"
    data_path.mkdir(parents=True, exist_ok=True)

    t0 = time.time()

    countries = download_countries(data_path)
    download_regions(data_path, countries)

    total_records = 0
    for category, indicators in ALL_CATEGORIES.items():
        count = download_indicator_category(data_path, category, indicators)
        total_records += count

    elapsed = time.time() - t0
    print(f"\n--- Download complete in {elapsed:.0f}s ---")
    print(f"Total indicator records: {total_records}")
    for f in sorted(data_path.glob("*.json")):
        size = f.stat().st_size
        print(f"  {f.name}: {size / 1024:.0f} KB")

    return {"countries": len(countries), "records": total_records, "elapsed_s": round(elapsed, 1)}


def main():
    parser = argparse.ArgumentParser(description="Download World Bank WDI data")
    parser.add_argument("--data-dir", default="data", help="Output directory")
    args = parser.parse_args()
    download_all(args.data_dir)


if __name__ == "__main__":
    main()
