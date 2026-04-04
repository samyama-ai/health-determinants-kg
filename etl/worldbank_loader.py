"""World Bank WDI Health Determinants loader.

Loads countries, regions, and indicator data across 5 categories
(socioeconomic, environmental, nutrition, demographic, water)
from pre-downloaded World Bank JSON files into Samyama graph.

Usage:
    from etl.worldbank_loader import load_worldbank_data
    load_worldbank_data(client, data_dir, registry)
"""

from __future__ import annotations

import json
import os

from etl.helpers import (
    Registry,
    batch_create_nodes,
    batch_create_edges,
    batch_create_edges_fast,
    create_index,
)

# Category -> (node_label, edge_type, id_prefix, registry_node_field, registry_edge_field)
CATEGORY_CONFIG = {
    "socioeconomic": ("SocioeconomicIndicator", "HAS_INDICATOR", "SE",
                      "socioeconomic_indicators", "has_indicator"),
    "environmental": ("EnvironmentalFactor", "ENVIRONMENT_OF", "EF",
                      "environmental_factors", "environment_of"),
    "nutrition": ("NutritionIndicator", "NUTRITION_STATUS", "NI",
                  "nutrition_indicators", "nutrition_status"),
    "demographic": ("DemographicProfile", "DEMOGRAPHIC_OF", "DP",
                    "demographic_profiles", "demographic_of"),
    "water": ("WaterResource", "WATER_RESOURCE_OF", "WR",
              "water_resources", "water_resource_of"),
}


def _load_indicator_category(
    client,
    data_dir: str,
    category: str,
    registry: Registry,
    tenant: str,
) -> tuple[int, int]:
    """Load a single indicator category. Returns (node_count, edge_count)."""
    label, edge_type, prefix, reg_node, reg_edge = CATEGORY_CONFIG[category]

    data_path = os.path.join(data_dir, "worldbank", f"{category}.json")
    if not os.path.exists(data_path):
        return 0, 0

    with open(data_path) as f:
        records = json.load(f)

    node_registry = getattr(registry, reg_node)
    edge_registry = getattr(registry, reg_edge)

    node_batch = []
    edge_batch = []

    for rec in records:
        country_info = rec.get("country", {})
        country_code = country_info.get("id", "").strip()
        indicator_info = rec.get("indicator", {})
        indicator_code = indicator_info.get("id", "").strip()
        indicator_name = indicator_info.get("value", "").strip()
        date = rec.get("date", "").strip()
        value = rec.get("value")

        if not country_code or not indicator_code or not date or value is None:
            continue

        # Skip aggregates (non-country codes are 2-char or region codes)
        if country_code not in registry.countries:
            continue

        nid = f"{prefix}-{country_code}-{indicator_code}-{date}"

        if nid not in node_registry:
            node_registry.add(nid)
            props = {
                "id": nid,
                "indicator_code": indicator_code,
                "indicator_name": indicator_name,
                "year": int(date) if date.isdigit() else date,
                "value": value,
                "category": category,
            }
            node_batch.append((label, props))

        edge_key = f"{country_code}|{nid}"
        if edge_key not in edge_registry:
            edge_registry.add(edge_key)
            edge_batch.append((
                "Country", "iso_code", country_code,
                label, "id", nid,
                edge_type, {},
            ))

    # Batch create nodes in chunks of 100
    node_count = 0
    for i in range(0, len(node_batch), 100):
        chunk = node_batch[i:i + 100]
        batch_create_nodes(client, chunk, tenant)
        node_count += len(chunk)

    # Batch create edges
    edge_count = batch_create_edges_fast(client, edge_batch, tenant)

    return node_count, edge_count


def load_worldbank_data(
    client,
    data_dir: str,
    registry: Registry,
    tenant: str = "default",
) -> dict:
    """Load World Bank WDI data into the graph.

    Expects JSON files in data_dir/worldbank/:
      countries.json, regions.json,
      socioeconomic.json, environmental.json, nutrition.json,
      demographic.json, water.json
    """
    print("Health Determinants KG: World Bank WDI")

    # --- Indexes ---
    create_index(client, "Country", "iso_code", tenant)
    create_index(client, "Country", "name", tenant)
    create_index(client, "Region", "code", tenant)
    create_index(client, "Region", "name", tenant)
    for cat_config in CATEGORY_CONFIG.values():
        create_index(client, cat_config[0], "id", tenant)

    country_nodes = 0
    region_nodes = 0
    in_region_edges = 0

    # ── Countries ──
    countries_path = os.path.join(data_dir, "worldbank", "countries.json")
    if os.path.exists(countries_path):
        with open(countries_path) as f:
            countries = json.load(f)
        batch = []
        for c in countries:
            code = c.get("id", "").strip()
            name = c.get("name", "").strip()
            if not code or not name:
                continue
            if code in registry.countries:
                continue
            registry.countries.add(code)
            region = c.get("region", {})
            income = c.get("incomeLevel", {})
            props = {
                "iso_code": code,
                "name": name,
                "income_level": income.get("value", ""),
                "region_wb": region.get("value", ""),
            }
            batch.append(("Country", props))
            if len(batch) >= 50:
                batch_create_nodes(client, batch, tenant)
                country_nodes += len(batch)
                batch = []
        if batch:
            batch_create_nodes(client, batch, tenant)
            country_nodes += len(batch)
        print(f"  Countries: {country_nodes}")

    # ── Regions ──
    regions_path = os.path.join(data_dir, "worldbank", "regions.json")
    if os.path.exists(regions_path):
        with open(regions_path) as f:
            regions = json.load(f)
        batch = []
        for r in regions:
            code = r.get("code", "").strip()
            name = r.get("name", "").strip()
            if not code or not name:
                continue
            if code in registry.regions:
                continue
            registry.regions.add(code)
            batch.append(("Region", {"code": code, "name": name}))
        if batch:
            batch_create_nodes(client, batch, tenant)
        region_nodes = len(batch)
        print(f"  Regions: {region_nodes}")

    # ── Country → Region edges ──
    if os.path.exists(countries_path):
        with open(countries_path) as f:
            countries = json.load(f)
        edge_batch = []
        for c in countries:
            code = c.get("id", "").strip()
            region = c.get("region", {})
            region_code = region.get("id", "").strip()
            if not code or not region_code:
                continue
            if code not in registry.countries:
                continue
            edge_key = f"{code}|{region_code}"
            if edge_key in registry.in_region:
                continue
            registry.in_region.add(edge_key)
            edge_batch.append((
                "Country", "iso_code", code,
                "Region", "code", region_code,
                "IN_REGION", {},
            ))
        in_region_edges = batch_create_edges(client, edge_batch, tenant)
        print(f"  IN_REGION edges: {in_region_edges}")

    # ── Indicator categories ──
    category_stats = {}
    for category in CATEGORY_CONFIG:
        nodes, edges = _load_indicator_category(
            client, data_dir, category, registry, tenant
        )
        category_stats[category] = {"nodes": nodes, "edges": edges}
        print(f"  {category}: {nodes} nodes, {edges} edges")

    stats = {
        "source": "worldbank_wdi",
        "country_nodes": country_nodes,
        "region_nodes": region_nodes,
        "socioeconomic_nodes": category_stats.get("socioeconomic", {}).get("nodes", 0),
        "environmental_nodes": category_stats.get("environmental", {}).get("nodes", 0),
        "nutrition_nodes": category_stats.get("nutrition", {}).get("nodes", 0),
        "demographic_nodes": category_stats.get("demographic", {}).get("nodes", 0),
        "water_nodes": category_stats.get("water", {}).get("nodes", 0),
        "in_region_edges": in_region_edges,
        "has_indicator_edges": category_stats.get("socioeconomic", {}).get("edges", 0),
        "environment_of_edges": category_stats.get("environmental", {}).get("edges", 0),
        "nutrition_status_edges": category_stats.get("nutrition", {}).get("edges", 0),
        "demographic_of_edges": category_stats.get("demographic", {}).get("edges", 0),
        "water_resource_of_edges": category_stats.get("water", {}).get("edges", 0),
    }

    total_nodes = country_nodes + region_nodes + sum(
        cs.get("nodes", 0) for cs in category_stats.values()
    )
    total_edges = in_region_edges + sum(
        cs.get("edges", 0) for cs in category_stats.values()
    )
    print(f"  Total: {total_nodes} nodes, {total_edges} edges")
    return stats
