"""Tests for World Bank WDI health determinants loader.

TDD: sample fixture data mimicking World Bank API responses.
"""

import json
import os
import tempfile

import pytest


# --- Sample World Bank data fixtures ---

SAMPLE_COUNTRIES = [
    {"id": "IND", "iso2Code": "IN", "name": "India",
     "region": {"id": "SAS", "value": "South Asia"},
     "incomeLevel": {"id": "LMC", "value": "Lower middle income"}},
    {"id": "NGA", "iso2Code": "NG", "name": "Nigeria",
     "region": {"id": "SSF", "value": "Sub-Saharan Africa"},
     "incomeLevel": {"id": "LMC", "value": "Lower middle income"}},
    {"id": "BRA", "iso2Code": "BR", "name": "Brazil",
     "region": {"id": "LCN", "value": "Latin America & Caribbean"},
     "incomeLevel": {"id": "UMC", "value": "Upper middle income"}},
]

SAMPLE_REGIONS = [
    {"code": "SAS", "name": "South Asia"},
    {"code": "SSF", "name": "Sub-Saharan Africa"},
    {"code": "LCN", "name": "Latin America & Caribbean"},
]

SAMPLE_SOCIOECONOMIC = [
    {"country": {"id": "IND"}, "indicator": {"id": "NY.GDP.PCAP.CD", "value": "GDP per capita (current US$)"},
     "date": "2023", "value": 2389.0},
    {"country": {"id": "IND"}, "indicator": {"id": "SI.POV.DDAY", "value": "Poverty headcount ratio at $2.15/day"},
     "date": "2021", "value": 12.0},
    {"country": {"id": "NGA"}, "indicator": {"id": "NY.GDP.PCAP.CD", "value": "GDP per capita (current US$)"},
     "date": "2023", "value": 1621.0},
    {"country": {"id": "BRA"}, "indicator": {"id": "NY.GDP.PCAP.CD", "value": "GDP per capita (current US$)"},
     "date": "2023", "value": 10219.0},
]

SAMPLE_ENVIRONMENTAL = [
    {"country": {"id": "IND"}, "indicator": {"id": "EN.ATM.PM25.MC.M3", "value": "PM2.5 air pollution (ug/m3)"},
     "date": "2021", "value": 53.3},
    {"country": {"id": "NGA"}, "indicator": {"id": "EN.ATM.PM25.MC.M3", "value": "PM2.5 air pollution (ug/m3)"},
     "date": "2021", "value": 46.5},
    {"country": {"id": "BRA"}, "indicator": {"id": "EN.ATM.CO2.KT", "value": "CO2 emissions (kt)"},
     "date": "2021", "value": 476321.0},
]

SAMPLE_NUTRITION = [
    {"country": {"id": "IND"}, "indicator": {"id": "SH.STA.STNT.ZS", "value": "Stunting prevalence (%)"},
     "date": "2021", "value": 31.7},
    {"country": {"id": "NGA"}, "indicator": {"id": "SH.STA.STNT.ZS", "value": "Stunting prevalence (%)"},
     "date": "2021", "value": 31.5},
]

SAMPLE_DEMOGRAPHIC = [
    {"country": {"id": "IND"}, "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
     "date": "2023", "value": 1428627663.0},
    {"country": {"id": "NGA"}, "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
     "date": "2023", "value": 223804632.0},
    {"country": {"id": "BRA"}, "indicator": {"id": "SP.DYN.LE00.IN", "value": "Life expectancy at birth"},
     "date": "2022", "value": 75.3},
]

SAMPLE_WATER = [
    {"country": {"id": "IND"}, "indicator": {"id": "SH.H2O.SMDW.ZS", "value": "Safely managed drinking water (%)"},
     "date": "2022", "value": 59.0},
    {"country": {"id": "NGA"}, "indicator": {"id": "SH.H2O.SMDW.ZS", "value": "Safely managed drinking water (%)"},
     "date": "2022", "value": 19.0},
]


def _write_fixture(tmpdir, subdir, filename, data):
    path = os.path.join(tmpdir, subdir)
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, filename)
    with open(filepath, "w") as f:
        json.dump(data, f)
    return filepath


@pytest.fixture(scope="module")
def worldbank_data():
    """Create fixture data and load into embedded graph."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wb_dir = os.path.join(tmpdir, "worldbank")
        os.makedirs(wb_dir, exist_ok=True)

        _write_fixture(tmpdir, "worldbank", "countries.json", SAMPLE_COUNTRIES)
        _write_fixture(tmpdir, "worldbank", "regions.json", SAMPLE_REGIONS)
        _write_fixture(tmpdir, "worldbank", "socioeconomic.json", SAMPLE_SOCIOECONOMIC)
        _write_fixture(tmpdir, "worldbank", "environmental.json", SAMPLE_ENVIRONMENTAL)
        _write_fixture(tmpdir, "worldbank", "nutrition.json", SAMPLE_NUTRITION)
        _write_fixture(tmpdir, "worldbank", "demographic.json", SAMPLE_DEMOGRAPHIC)
        _write_fixture(tmpdir, "worldbank", "water.json", SAMPLE_WATER)

        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.worldbank_loader import load_worldbank_data

            client = SamyamaClient.embedded()
            registry = Registry()
            stats = load_worldbank_data(client, tmpdir, registry)
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


class TestCountryNodes:
    def test_countries_created(self, worldbank_data):
        client, stats, _ = worldbank_data
        rows = _q(client, "MATCH (c:Country) RETURN c.name ORDER BY c.name")
        names = [r["c.name"] for r in rows]
        assert "India" in names
        assert "Nigeria" in names
        assert "Brazil" in names

    def test_country_count(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, "MATCH (c:Country) RETURN count(*) AS c")
        assert rows[0]["c"] == 3

    def test_country_has_iso_code(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, "MATCH (c:Country {name: 'India'}) RETURN c.iso_code")
        assert rows[0]["c.iso_code"] == "IND"

    def test_country_has_income_level(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, "MATCH (c:Country {name: 'Brazil'}) RETURN c.income_level")
        assert rows[0]["c.income_level"] == "Upper middle income"


class TestRegionNodes:
    def test_regions_created(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, "MATCH (r:Region) RETURN r.name ORDER BY r.name")
        names = [r["r.name"] for r in rows]
        assert "South Asia" in names
        assert "Sub-Saharan Africa" in names

    def test_in_region_edges(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, """
            MATCH (c:Country {name: 'India'})-[:IN_REGION]->(r:Region)
            RETURN r.name
        """)
        assert rows[0]["r.name"] == "South Asia"


class TestSocioeconomicIndicators:
    def test_indicators_created(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, "MATCH (s:SocioeconomicIndicator) RETURN count(*) AS c")
        assert rows[0]["c"] >= 4

    def test_linked_to_country(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, """
            MATCH (c:Country {name: 'India'})-[:HAS_INDICATOR]->(s:SocioeconomicIndicator)
            RETURN s.indicator_name, s.value, s.year
            ORDER BY s.year DESC
        """)
        assert len(rows) >= 2
        gdp_rows = [r for r in rows if "GDP" in str(r.get("s.indicator_name", ""))]
        assert len(gdp_rows) >= 1

    def test_composite_id(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, """
            MATCH (s:SocioeconomicIndicator)
            WHERE s.id STARTS WITH 'SE-IND'
            RETURN s.id LIMIT 1
        """)
        assert len(rows) >= 1
        assert rows[0]["s.id"].startswith("SE-IND-")


class TestEnvironmentalFactors:
    def test_factors_created(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, "MATCH (e:EnvironmentalFactor) RETURN count(*) AS c")
        assert rows[0]["c"] >= 3

    def test_linked_to_country(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, """
            MATCH (c:Country {name: 'India'})-[:ENVIRONMENT_OF]->(e:EnvironmentalFactor)
            RETURN e.indicator_name, e.value
        """)
        assert len(rows) >= 1


class TestNutritionIndicators:
    def test_nutrition_created(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, "MATCH (n:NutritionIndicator) RETURN count(*) AS c")
        assert rows[0]["c"] >= 2

    def test_linked_to_country(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, """
            MATCH (c:Country {name: 'India'})-[:NUTRITION_STATUS]->(n:NutritionIndicator)
            RETURN n.indicator_name, n.value
        """)
        assert len(rows) >= 1


class TestDemographicProfiles:
    def test_demographics_created(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, "MATCH (d:DemographicProfile) RETURN count(*) AS c")
        assert rows[0]["c"] >= 3

    def test_linked_to_country(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, """
            MATCH (c:Country {name: 'India'})-[:DEMOGRAPHIC_OF]->(d:DemographicProfile)
            RETURN d.indicator_name, d.value
        """)
        assert len(rows) >= 1


class TestWaterResources:
    def test_water_created(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, "MATCH (w:WaterResource) RETURN count(*) AS c")
        assert rows[0]["c"] >= 2

    def test_linked_to_country(self, worldbank_data):
        client, _, _ = worldbank_data
        rows = _q(client, """
            MATCH (c:Country {name: 'Nigeria'})-[:WATER_RESOURCE_OF]->(w:WaterResource)
            RETURN w.value
        """)
        assert len(rows) >= 1
        assert rows[0]["w.value"] == 19.0


class TestCrossQueries:
    def test_country_all_determinants(self, worldbank_data):
        """A country should have indicators across multiple categories."""
        client, _, _ = worldbank_data
        rows = _q(client, """
            MATCH (c:Country {name: 'India'})-[:HAS_INDICATOR]->(s:SocioeconomicIndicator)
            RETURN count(s) AS se_count
        """)
        se_count = rows[0]["se_count"]
        rows2 = _q(client, """
            MATCH (c:Country {name: 'India'})-[:ENVIRONMENT_OF]->(e:EnvironmentalFactor)
            RETURN count(e) AS ef_count
        """)
        ef_count = rows2[0]["ef_count"]
        assert se_count >= 1
        assert ef_count >= 1

    def test_low_water_access(self, worldbank_data):
        """Find countries with low water access."""
        client, _, _ = worldbank_data
        rows = _q(client, """
            MATCH (c:Country)-[:WATER_RESOURCE_OF]->(w:WaterResource)
            WHERE w.value < 30
            RETURN c.name, w.value
        """)
        assert len(rows) >= 1
        assert any(r["c.name"] == "Nigeria" for r in rows)


class TestStats:
    def test_stats_returned(self, worldbank_data):
        _, stats, _ = worldbank_data
        assert stats["source"] == "worldbank_wdi"
        assert stats["country_nodes"] == 3
        assert stats["region_nodes"] == 3
        assert stats["socioeconomic_nodes"] >= 4
        assert stats["environmental_nodes"] >= 3
        assert stats["nutrition_nodes"] >= 2
        assert stats["demographic_nodes"] >= 3
        assert stats["water_nodes"] >= 2

    def test_edge_counts(self, worldbank_data):
        _, stats, _ = worldbank_data
        assert stats["in_region_edges"] == 3
        assert stats["has_indicator_edges"] >= 4
        assert stats["environment_of_edges"] >= 3
