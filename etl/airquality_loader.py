"""WHO Air Quality loader.

Loads city-level PM2.5/PM10 data as EnvironmentalFactor nodes
linked to existing Country nodes via ENVIRONMENT_OF edges.

Usage:
    from etl.airquality_loader import load_air_quality
    load_air_quality(client, data_dir, registry)
"""

from __future__ import annotations

import csv
import os

from etl.helpers import (
    Registry,
    batch_create_nodes,
    batch_create_edges_fast,
    create_index,
)


def load_air_quality(
    client,
    data_dir: str,
    registry: Registry,
    tenant: str = "default",
) -> dict:
    """Load WHO Air Quality data into the graph.

    Expects: data_dir/who_airquality/air_quality.csv
    Columns: country_code, city, year, pm25, pm10
    """
    print("Health Determinants KG: WHO Air Quality")

    create_index(client, "EnvironmentalFactor", "id", tenant)

    csv_path = os.path.join(data_dir, "who_airquality", "air_quality.csv")
    if not os.path.exists(csv_path):
        print("  No air quality data found, skipping")
        return {"source": "who_air_quality", "environmental_nodes": 0, "environment_of_edges": 0}

    node_batch = []
    edge_batch = []

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country_code = row.get("country_code", "").strip()
            city = row.get("city", "").strip()
            year = row.get("year", "").strip()
            pm25 = row.get("pm25", "").strip()
            pm10 = row.get("pm10", "").strip()

            if not country_code or not year:
                continue
            if country_code not in registry.countries:
                continue

            nid = f"EF-{country_code}-AQ-{city}-{year}"
            if nid in registry.environmental_factors:
                continue
            registry.environmental_factors.add(nid)

            props = {
                "id": nid,
                "indicator_code": "AIR_QUALITY",
                "indicator_name": "Ambient air quality",
                "year": int(year) if year.isdigit() else year,
                "category": "air_quality",
                "city": city,
            }
            if pm25 and pm25 != "None":
                try:
                    props["value"] = float(pm25)
                except ValueError:
                    pass
            if pm10 and pm10 != "None":
                try:
                    props["pm10"] = float(pm10)
                except ValueError:
                    pass

            node_batch.append(("EnvironmentalFactor", props))

            edge_key = f"{country_code}|{nid}"
            if edge_key not in registry.environment_of:
                registry.environment_of.add(edge_key)
                edge_batch.append((
                    "Country", "iso_code", country_code,
                    "EnvironmentalFactor", "id", nid,
                    "ENVIRONMENT_OF", {},
                ))

    node_count = 0
    for i in range(0, len(node_batch), 100):
        chunk = node_batch[i:i + 100]
        batch_create_nodes(client, chunk, tenant)
        node_count += len(chunk)

    edge_count = batch_create_edges_fast(client, edge_batch, tenant)

    print(f"  Air quality: {node_count} nodes, {edge_count} edges")

    return {
        "source": "who_air_quality",
        "environmental_nodes": node_count,
        "environment_of_edges": edge_count,
    }
