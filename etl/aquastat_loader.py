"""FAO AQUASTAT water resources loader.

Loads freshwater withdrawal, water stress, and related indicators
as WaterResource nodes linked to Country nodes.

Usage:
    from etl.aquastat_loader import load_aquastat
    load_aquastat(client, data_dir, registry)
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


def load_aquastat(
    client,
    data_dir: str,
    registry: Registry,
    tenant: str = "default",
) -> dict:
    """Load FAO AQUASTAT data into the graph.

    Expects: data_dir/fao/aquastat.csv
    Columns: country_code, indicator_code, indicator_name, year, value
    """
    print("Health Determinants KG: FAO AQUASTAT")

    create_index(client, "WaterResource", "id", tenant)

    csv_path = os.path.join(data_dir, "fao", "aquastat.csv")
    if not os.path.exists(csv_path):
        print("  No AQUASTAT data found, skipping")
        return {"source": "fao_aquastat", "water_nodes": 0, "water_resource_of_edges": 0}

    node_batch = []
    edge_batch = []

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country_code = row.get("country_code", "").strip()
            indicator_code = row.get("indicator_code", "").strip()
            indicator_name = row.get("indicator_name", "").strip()
            year = row.get("year", "").strip()
            value_str = row.get("value", "").strip()

            if not country_code or not indicator_code or not year or not value_str:
                continue
            if country_code not in registry.countries:
                continue

            try:
                value = float(value_str)
            except ValueError:
                continue

            nid = f"WR-{country_code}-{indicator_code}-{year}"
            if nid in registry.water_resources:
                continue
            registry.water_resources.add(nid)

            props = {
                "id": nid,
                "indicator_code": indicator_code,
                "indicator_name": indicator_name,
                "year": int(year) if year.isdigit() else year,
                "value": value,
                "category": "water",
            }
            node_batch.append(("WaterResource", props))

            edge_key = f"{country_code}|{nid}"
            if edge_key not in registry.water_resource_of:
                registry.water_resource_of.add(edge_key)
                edge_batch.append((
                    "Country", "iso_code", country_code,
                    "WaterResource", "id", nid,
                    "WATER_RESOURCE_OF", {},
                ))

    node_count = 0
    for i in range(0, len(node_batch), 100):
        chunk = node_batch[i:i + 100]
        batch_create_nodes(client, chunk, tenant)
        node_count += len(chunk)

    edge_count = batch_create_edges_fast(client, edge_batch, tenant)

    print(f"  AQUASTAT: {node_count} nodes, {edge_count} edges")

    return {
        "source": "fao_aquastat",
        "water_nodes": node_count,
        "water_resource_of_edges": edge_count,
    }
