"""Render D3 charts inside Streamlit (``st.iframe`` / ``components.html``).

Design contract (see ``streamlit/D3_PROPOSAL.md`` section 6):

* Python owns querying and filtering; JavaScript only ever receives the
  already-filtered payload.
* A chart is a JS snippet at ``components/charts/<name>.js``. It is evaluated
  inside an IIFE that provides:

  - ``target``  -- the mount ``<div>`` element
  - ``payload`` -- ``{"data": <records>, "config": <chart options>}``
  - ``d3``      -- the D3 v7 global loaded from CDN

  The snippet is expected to render into ``target`` and clear it first.
"""

import json
import re
import uuid
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

_D3_CDN = "https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"
_CHARTS_DIR = Path(__file__).resolve().parent / "charts"
_CHART_NAME = re.compile(r"^[a-z0-9_]+$")
# Closing script tags must never appear literally inside an inline <script>.
_UNSAFE_HTML = re.compile(r"</")


def _to_json(data) -> str:
    """Serialize a DataFrame or list of records to JSON safe for <script>."""
    if hasattr(data, "to_json"):
        raw = data.to_json(orient="records", date_format="iso")
    else:
        raw = json.dumps(data, default=str)
    return _UNSAFE_HTML.sub("<\\/", raw)


def _payload_json(data, config) -> str:
    """Build the ``{"data": ..., "config": ...}`` object as a JSON literal."""
    return '{"data": %s, "config": %s}' % (
        _to_json(data),
        _to_json(config or {}),
    )


def _load_chart(chart: str) -> str:
    if not isinstance(chart, str) or not _CHART_NAME.fullmatch(chart):
        raise ValueError(
            f"Invalid chart name {chart!r}: expected a bare filename stem "
            "matching [a-z0-9_]+"
        )
    path = _CHARTS_DIR / f"{chart}.js"
    if not path.is_file():
        available = sorted(p.stem for p in _CHARTS_DIR.glob("*.js"))
        raise ValueError(
            f"Unknown D3 chart {chart!r}: expected {path} "
            f"(available: {', '.join(available) or 'none'})"
        )
    return path.read_text(encoding="utf-8")


def _mount(html: str, height: int) -> None:
    """Embed the chart HTML in an iframe (``st.iframe`` on newer Streamlit)."""
    iframe = getattr(st, "iframe", None)
    if iframe is not None:
        iframe(html, height=height)
    else:
        components.html(html, height=height)


def render_d3(chart: str, data, *, height: int = 400, config: dict | None = None):
    """Render a D3 chart from a JS template plus JSON data.

    Args:
        chart: filename stem of a template under ``components/charts/``
            (e.g. ``"heatmap"``).
        data: pandas DataFrame or list of dicts; serialized to JSON records.
        height: height in pixels of the component iframe.
        config: chart-specific options (colors, labels, sizing).
    """
    js = _load_chart(chart)
    payload = _payload_json(data, config)
    mount_id = f"d3-{uuid.uuid4().hex[:12]}"

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  html, body {{ margin: 0; padding: 0; overflow: hidden; }}
  .d3-chart {{ box-sizing: border-box; width: 100%; }}
</style>
</head>
<body>
<div id="{mount_id}" class="d3-chart" style="height: {height}px;"></div>
<script src="{_D3_CDN}"></script>
<script>
(function () {{
  "use strict";
  var target = document.getElementById("{mount_id}");
  var payload = {payload};
  {js}
}})();
</script>
</body>
</html>"""
    _mount(html, height)
