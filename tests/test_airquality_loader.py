"""Tests for WHO Air Quality loader.

TDD: sample fixture data mimicking WHO Air Quality CSV.
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

SAMPLE_AIR_QUALITY = [
    {"country_code": "IND", "city": "Delhi", "year": 2022, "pm25": 96.4, "pm10": 192.0},
    {"country_code": "IND", "city": "Mumbai", "year": 2022, "pm25": 37.2, "pm10": 84.0},
    {"country_code": "NGA", "city": "Lagos", "year": 2022, "pm25": 68.0, "pm10": None},
]


def _write_json(tmpdir, subdir, filename, data):
    path = os.path.join(tmpdir, subdir)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, filename), "w") as f:
        json.dump(data, f)


def _write_csv(tmpdir, subdir, filename, rows):
    path = os.path.join(tmpdir, subdir)
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, filename)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture(scope="module")
def airquality_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_json(tmpdir, "worldbank", "countries.json", SAMPLE_COUNTRIES)
        _write_json(tmpdir, "worldbank", "regions.json", SAMPLE_REGIONS)
        _write_csv(tmpdir, "who_airquality", "air_quality.csv", SAMPLE_AIR_QUALITY)

        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.worldbank_loader import load_worldbank_data
            from etl.airquality_loader import load_air_quality

            client = SamyamaClient.embedded()
            registry = Registry()
            # Load countries first (worldbank phase provides Country nodes)
            load_worldbank_data(client, tmpdir, registry)
            stats = load_air_quality(client, tmpdir, registry)
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


class TestAirQualityNodes:
    def test_nodes_created(self, airquality_data):
        client, stats, _ = airquality_data
        rows = _q(client, "MATCH (e:EnvironmentalFactor) WHERE e.category = 'air_quality' RETURN count(*) AS c")
        assert rows[0]["c"] >= 3

    def test_pm25_value(self, airquality_data):
        client, _, _ = airquality_data
        rows = _q(client, """
            MATCH (e:EnvironmentalFactor)
            WHERE e.city = 'Delhi'
            RETURN e.value
        """)
        assert len(rows) >= 1
        assert rows[0]["e.value"] == 96.4

    def test_linked_to_country(self, airquality_data):
        client, _, _ = airquality_data
        rows = _q(client, """
            MATCH (c:Country {name: 'India'})-[:ENVIRONMENT_OF]->(e:EnvironmentalFactor)
            WHERE e.category = 'air_quality'
            RETURN e.city, e.value
        """)
        assert len(rows) >= 2

    def test_stats(self, airquality_data):
        _, stats, _ = airquality_data
        assert stats["source"] == "who_air_quality"
        assert stats["environmental_nodes"] >= 3
        assert stats["environment_of_edges"] >= 3
