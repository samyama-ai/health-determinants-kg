"""UNDP Human Development Index (HDI) loader.

Loads HDI values as SocioeconomicIndicator nodes linked to Country nodes.

Usage:
    from etl.hdi_loader import load_hdi
    load_hdi(client, data_dir, registry)
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


def load_hdi(
    client,
    data_dir: str,
    registry: Registry,
    tenant: str = "default",
) -> dict:
    """Load UNDP HDI data into the graph.

    Expects: data_dir/undp/hdi.csv
    Columns: country_code, year, hdi, rank
    """
    print("Health Determinants KG: UNDP HDI")

    create_index(client, "SocioeconomicIndicator", "id", tenant)

    csv_path = os.path.join(data_dir, "undp", "hdi.csv")
    if not os.path.exists(csv_path):
        print("  No HDI data found, skipping")
        return {"source": "undp_hdi", "hdi_nodes": 0, "has_indicator_edges": 0}

    node_batch = []
    edge_batch = []

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country_code = row.get("country_code", "").strip()
            year = row.get("year", "").strip()
            hdi_str = row.get("hdi", "").strip()
            rank_str = row.get("rank", "").strip()

            if not country_code or not year or not hdi_str:
                continue
            if country_code not in registry.countries:
                continue

            try:
                hdi_val = float(hdi_str)
            except ValueError:
                continue

            nid = f"SE-{country_code}-HDI-{year}"
            if nid in registry.socioeconomic_indicators:
                continue
            registry.socioeconomic_indicators.add(nid)

            props = {
                "id": nid,
                "indicator_code": "HDI",
                "indicator_name": "Human Development Index",
                "year": int(year) if year.isdigit() else year,
                "value": hdi_val,
                "category": "development",
            }
            if rank_str and rank_str.isdigit():
                props["rank"] = int(rank_str)

            node_batch.append(("SocioeconomicIndicator", props))

            edge_key = f"{country_code}|{nid}"
            if edge_key not in registry.has_indicator:
                registry.has_indicator.add(edge_key)
                edge_batch.append((
                    "Country", "iso_code", country_code,
                    "SocioeconomicIndicator", "id", nid,
                    "HAS_INDICATOR", {},
                ))

    node_count = 0
    for i in range(0, len(node_batch), 100):
        chunk = node_batch[i:i + 100]
        batch_create_nodes(client, chunk, tenant)
        node_count += len(chunk)

    edge_count = batch_create_edges_fast(client, edge_batch, tenant)

    print(f"  HDI: {node_count} nodes, {edge_count} edges")

    return {
        "source": "undp_hdi",
        "hdi_nodes": node_count,
        "has_indicator_edges": edge_count,
    }
