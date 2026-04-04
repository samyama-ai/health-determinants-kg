"""Tests for UNDP HDI loader.

TDD: sample fixture data mimicking UNDP HDI CSV.
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
    {"id": "NOR", "name": "Norway",
     "region": {"id": "ECS", "value": "Europe & Central Asia"},
     "incomeLevel": {"id": "HIC", "value": "High income"}},
]

SAMPLE_REGIONS = [
    {"code": "SAS", "name": "South Asia"},
    {"code": "SSF", "name": "Sub-Saharan Africa"},
    {"code": "ECS", "name": "Europe & Central Asia"},
]

SAMPLE_HDI = [
    {"country_code": "IND", "year": 2022, "hdi": 0.644, "rank": 134},
    {"country_code": "IND", "year": 2021, "hdi": 0.633, "rank": 132},
    {"country_code": "NGA", "year": 2022, "hdi": 0.548, "rank": 163},
    {"country_code": "NOR", "year": 2022, "hdi": 0.966, "rank": 1},
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
def hdi_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_json(tmpdir, "worldbank", "countries.json", SAMPLE_COUNTRIES)
        _write_json(tmpdir, "worldbank", "regions.json", SAMPLE_REGIONS)
        _write_csv(tmpdir, "undp", "hdi.csv", SAMPLE_HDI)

        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.worldbank_loader import load_worldbank_data
            from etl.hdi_loader import load_hdi

            client = SamyamaClient.embedded()
            registry = Registry()
            load_worldbank_data(client, tmpdir, registry)
            stats = load_hdi(client, tmpdir, registry)
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


class TestHDINodes:
    def test_nodes_created(self, hdi_data):
        client, _, _ = hdi_data
        rows = _q(client, """
            MATCH (s:SocioeconomicIndicator)
            WHERE s.indicator_code = 'HDI'
            RETURN count(*) AS c
        """)
        assert rows[0]["c"] >= 4

    def test_linked_to_country(self, hdi_data):
        client, _, _ = hdi_data
        rows = _q(client, """
            MATCH (c:Country {name: 'India'})-[:HAS_INDICATOR]->(s:SocioeconomicIndicator)
            WHERE s.indicator_code = 'HDI'
            RETURN s.value, s.year
            ORDER BY s.year DESC
        """)
        assert len(rows) >= 2
        assert rows[0]["s.value"] == 0.644

    def test_norway_top_hdi(self, hdi_data):
        client, _, _ = hdi_data
        rows = _q(client, """
            MATCH (c:Country {name: 'Norway'})-[:HAS_INDICATOR]->(s:SocioeconomicIndicator)
            WHERE s.indicator_code = 'HDI'
            RETURN s.value
        """)
        assert rows[0]["s.value"] == 0.966

    def test_stats(self, hdi_data):
        _, stats, _ = hdi_data
        assert stats["source"] == "undp_hdi"
        assert stats["hdi_nodes"] >= 4
        assert stats["has_indicator_edges"] >= 4
