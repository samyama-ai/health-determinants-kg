"""Tests for FAO AQUASTAT water resources loader.

TDD: sample fixture data mimicking FAO AQUASTAT CSV.
"""

import csv
import json
import os
import tempfile

import pytest


SAMPLE_COUNTRIES = [
    {"id": "IND", "name": "India",
     "region": {"id": "SAS", "value": "South Asia"},
     "incomeLevel": {"id": "LMC", "value": "Lower middle income"}},
    {"id": "NGA", "name": "Nigeria",
     "region": {"id": "SSF", "value": "Sub-Saharan Africa"},
     "incomeLevel": {"id": "LMC", "value": "Lower middle income"}},
]

SAMPLE_REGIONS = [
    {"code": "SAS", "name": "South Asia"},
    {"code": "SSF", "name": "Sub-Saharan Africa"},
]

SAMPLE_AQUASTAT = [
    {"country_code": "IND", "indicator_code": "FRESHWATER_WITHDRAWAL",
     "indicator_name": "Total freshwater withdrawal (billion m3/yr)",
     "year": 2020, "value": 647.5},
    {"country_code": "IND", "indicator_code": "WATER_STRESS",
     "indicator_name": "Water stress level (%)",
     "year": 2020, "value": 66.5},
    {"country_code": "NGA", "indicator_code": "FRESHWATER_WITHDRAWAL",
     "indicator_name": "Total freshwater withdrawal (billion m3/yr)",
     "year": 2020, "value": 13.1},
]


def _write_json(tmpdir, subdir, filename, data):
    path = os.path.join(tmpdir, subdir)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, filename), "w") as f:
        json.dump(data, f)


def _write_csv(tmpdir, subdir, filename, rows):
    path = os.path.join(tmpdir, subdir)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, filename), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture(scope="module")
def aquastat_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_json(tmpdir, "worldbank", "countries.json", SAMPLE_COUNTRIES)
        _write_json(tmpdir, "worldbank", "regions.json", SAMPLE_REGIONS)
        _write_csv(tmpdir, "fao", "aquastat.csv", SAMPLE_AQUASTAT)

        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.worldbank_loader import load_worldbank_data
            from etl.aquastat_loader import load_aquastat

            client = SamyamaClient.embedded()
            registry = Registry()
            load_worldbank_data(client, tmpdir, registry)
            stats = load_aquastat(client, tmpdir, registry)
            yield client, stats, registry
        except ImportError:
            pytest.skip("samyama package not available")


def _q(client, cypher):
    try:
        r = client.query_readonly(cypher, "default")
        return [dict(zip(r.columns, row)) for row in r.records]
    except Exception:
        r = client.query(cypher, "default")
        return [dict(zip(r.columns, row)) for row in r.records]


class TestWaterResourceNodes:
    def test_nodes_created(self, aquastat_data):
        client, _, _ = aquastat_data
        rows = _q(client, "MATCH (w:WaterResource) RETURN count(*) AS c")
        assert rows[0]["c"] >= 3

    def test_linked_to_country(self, aquastat_data):
        client, _, _ = aquastat_data
        rows = _q(client, """
            MATCH (c:Country {name: 'India'})-[:WATER_RESOURCE_OF]->(w:WaterResource)
            RETURN w.indicator_name, w.value
        """)
        assert len(rows) >= 2

    def test_water_stress_value(self, aquastat_data):
        client, _, _ = aquastat_data
        rows = _q(client, """
            MATCH (w:WaterResource)
            WHERE w.indicator_code = 'WATER_STRESS' AND w.id STARTS WITH 'WR-IND'
            RETURN w.value
        """)
        assert rows[0]["w.value"] == 66.5

    def test_stats(self, aquastat_data):
        _, stats, _ = aquastat_data
        assert stats["source"] == "fao_aquastat"
        assert stats["water_nodes"] >= 3
        assert stats["water_resource_of_edges"] >= 3
