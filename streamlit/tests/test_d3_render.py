"""Unit tests for the D3 render helper.

Standard-library ``unittest`` only (the repo has no Python test runner):

    cd streamlit && python -m unittest discover tests -v
"""

import json
import unittest

from components.d3_render import _load_chart, _payload_json, _to_json


class ToJsonTests(unittest.TestCase):
    def test_list_of_dicts(self):
        self.assertEqual(_to_json([{"a": 1}]), '[{"a": 1}]')

    def test_closing_script_tag_is_escaped(self):
        raw = _to_json([{"name": "</script><script>alert(1)</script>"}])
        self.assertNotIn("</script>", raw)
        self.assertIn("<\\/script>", raw)
        self.assertEqual(
            json.loads(raw.replace("<\\/", "</")),
            [{"name": "</script><script>alert(1)</script>"}],
        )

    def test_dataframe_serializes_to_records(self):
        pd = __import__("pandas")
        df = pd.DataFrame([{"Player": "Keaton", "1": 3.0, "Result 1": "Birdie"}])
        raw = _to_json(df)
        self.assertEqual(
            json.loads(raw),
            [{"Player": "Keaton", "1": 3, "Result 1": "Birdie"}],
        )

    def test_dataframe_nan_becomes_null(self):
        pd = __import__("pandas")
        df = pd.DataFrame([{"UDisc Rating": float("nan")}])
        self.assertEqual(json.loads(_to_json(df)), [{"UDisc Rating": None}])


class PayloadJsonTests(unittest.TestCase):
    def test_shape_is_valid_json(self):
        payload = json.loads(
            _payload_json([{"a": 1}], {"colors": {"Par": "#D3D3D3"}})
        )
        self.assertEqual(payload["data"], [{"a": 1}])
        self.assertEqual(payload["config"], {"colors": {"Par": "#D3D3D3"}})

    def test_missing_config_defaults_to_empty(self):
        self.assertEqual(json.loads(_payload_json([], None))["config"], {})


class LoadChartTests(unittest.TestCase):
    def test_loads_existing_chart(self):
        self.assertIn("d3", _load_chart("heatmap"))

    def test_loads_score_histogram(self):
        js = _load_chart("score_histogram")
        self.assertIn("d3", js)
        self.assertIn("Score", js)

    def test_loads_putting_radial(self):
        js = _load_chart("putting_radial")
        self.assertIn("d3", js)
        self.assertIn("made", js)

    def test_rejects_path_traversal(self):
        for bad in ("../secrets", "charts/heatmap", "Heatmap", "", None):
            with self.assertRaises(ValueError):
                _load_chart(bad)

    def test_unknown_chart_lists_available(self):
        with self.assertRaises(ValueError) as ctx:
            _load_chart("nope")
        self.assertIn("heatmap", str(ctx.exception))
        self.assertIn("score_histogram", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
