"""Tests for shared Cypher helpers."""

from etl.helpers import _escape, _q, _prop_str, Registry, ProgressReporter


class TestEscape:
    def test_plain_string(self):
        assert _escape("hello") == "hello"

    def test_single_quote(self):
        assert _escape("it's") == "it\\'s"

    def test_double_quote(self):
        assert _escape('say "hi"') == 'say \\"hi\\"'

    def test_backslash(self):
        assert _escape("a\\b") == "a\\\\b"

    def test_non_string(self):
        assert _escape(42) == "42"


class TestQ:
    def test_none(self):
        assert _q(None) == "null"

    def test_bool_true(self):
        assert _q(True) == "true"

    def test_bool_false(self):
        assert _q(False) == "false"

    def test_int(self):
        assert _q(42) == "42"

    def test_float(self):
        assert _q(3.14) == "3.14"

    def test_string(self):
        assert _q("hello") == "'hello'"

    def test_string_with_quote(self):
        assert _q("it's") == "'it\\'s'"


class TestPropStr:
    def test_empty(self):
        assert _prop_str({}) == "{}"

    def test_single(self):
        assert _prop_str({"name": "India"}) == "{name: 'India'}"

    def test_mixed_types(self):
        result = _prop_str({"name": "India", "pop": 1400000000, "hdi": 0.633})
        assert "name: 'India'" in result
        assert "pop: 1400000000" in result
        assert "hdi: 0.633" in result

    def test_none_values_skipped(self):
        result = _prop_str({"name": "India", "missing": None})
        assert "missing" not in result
        assert "name: 'India'" in result


class TestRegistry:
    def test_default_sets_empty(self):
        r = Registry()
        assert len(r.countries) == 0
        assert len(r.socioeconomic_indicators) == 0
        assert len(r.environmental_factors) == 0

    def test_dedup_tracking(self):
        r = Registry()
        r.countries.add("IND")
        r.countries.add("IND")  # duplicate
        assert len(r.countries) == 1

    def test_all_fields_are_sets(self):
        r = Registry()
        for field_name in [
            "countries", "regions",
            "socioeconomic_indicators", "environmental_factors",
            "nutrition_indicators", "demographic_profiles", "water_resources",
            "has_indicator", "environment_of", "nutrition_status",
            "demographic_of", "water_resource_of", "in_region",
        ]:
            assert isinstance(getattr(r, field_name), set)


class TestProgressReporter:
    def test_init(self):
        pr = ProgressReporter("test", total=100)
        assert pr.phase == "test"
        assert pr.total == 100
        assert pr.count == 0
        assert pr.errors == 0

    def test_tick(self):
        pr = ProgressReporter("test")
        pr.tick(5)
        assert pr.count == 5

    def test_summary(self):
        pr = ProgressReporter("test")
        pr.tick(10)
        s = pr.summary()
        assert s["phase"] == "test"
        assert s["processed"] == 10
        assert s["errors"] == 0
