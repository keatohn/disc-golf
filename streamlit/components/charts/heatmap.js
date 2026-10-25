/* Hole-result heatmap: one row per round, one cell per hole.
 *
 * Available in scope (injected by components/d3_render.py):
 *   target  -- mount element (must be cleared before rendering)
 *   payload -- {data: [round records], config: {...}}
 *   d3      -- D3 v7 global
 *
 * Record shape (from analytics.rounds):
 *   "Player", "Course Layout Name", "Date Formatted", "Score Display",
 *   "UDisc Rating", "1".."27" (strokes), "Result 1".."Result 27".
 */
(function () {
  var cfg = Object.assign(
    {
      colors: {},
      defaultColor: "#CCCCCC",
      otherLabel: "Other",
      cell: 22,
      radius: 4,
      gapX: 5,
      rowHeight: 30,
      gutter: 236,
      headerHeight: 30,
      maxHoles: 27,
      legend: true,
      legendGap: 14,
      legendRow: 18,
      meta: [
        { key: "Date Formatted", label: "Date", width: 104, bold: false },
        { key: "Score Display", label: "Score", width: 60, bold: true },
        { key: "UDisc Rating", label: "Rating", width: 64, bold: false }
      ]
    },
    payload.config || {}
  );

  var data = (payload && payload.data) || [];

  while (target.firstChild) target.removeChild(target.firstChild);
  target.style.position = "relative";
  target.style.overflow = "auto";

  if (!data.length) {
    var empty = document.createElement("div");
    empty.style.cssText = "padding:12px;font-size:13px;opacity:.7;";
    empty.textContent = "No rows to display";
    target.appendChild(empty);
    return;
  }

  var dark =
    cfg.theme && typeof cfg.theme.dark === "boolean"
      ? cfg.theme.dark
      : !!(window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
  var labelColor = dark ? "#E8E8E8" : "#1F2933";
  var mutedColor = dark ? "#9CA3AF" : "#5B6470";
  var gridHeaderColor = dark ? "#B0B7C3" : "#5B6470";

  function textWidth(chars, size) {
    return chars * size * 0.58;
  }

  function clip(text, maxChars) {
    var s = text === null || text === undefined ? "" : String(text);
    return s.length > maxChars ? s.slice(0, Math.max(1, maxChars - 1)) + "…" : s;
  }

  function luminance(color) {
    var c = d3.rgb(color);
    var channel = function (v) {
      v /= 255;
      return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
    };
    return 0.2126 * channel(c.r) + 0.7152 * channel(c.g) + 0.0722 * channel(c.b);
  }

  function fillFor(result) {
    if (result && Object.prototype.hasOwnProperty.call(cfg.colors, String(result))) {
      return cfg.colors[String(result)];
    }
    if (result && cfg.colorAliases && cfg.colorAliases[String(result)]) {
      return cfg.colorAliases[String(result)];
    }
    return cfg.defaultColor;
  }

  function cellText(value) {
    if (value === null || value === undefined || value === "") return "";
    var n = Number(value);
    if (!isFinite(n)) return String(value);
    return Number.isInteger(n) ? String(n) : n.toFixed(1);
  }

  // Largest hole index with a value in any row.
  var holeCount = 0;
  data.forEach(function (row) {
    for (var i = 1; i <= cfg.maxHoles; i += 1) {
      var v = row[String(i)];
      if (v !== null && v !== undefined && v !== "") holeCount = i;
    }
  });
  holeCount = Math.max(holeCount, 1);

  var metaWidth = cfg.meta.reduce(function (sum, m) {
    return sum + m.width;
  }, 0);
  var step = cfg.cell + cfg.gapX;
  var gridX = cfg.gutter + metaWidth + 12;
  var svgWidth = gridX + holeCount * step + 10;
  var rowsHeight = data.length * cfg.rowHeight;

  // Legend items: configured colors actually present, plus the fallback.
  var present = {};
  data.forEach(function (row) {
    for (var i = 1; i <= holeCount; i += 1) {
      var v = row["Result " + i];
      if (v !== null && v !== undefined && v !== "") present[String(v)] = true;
    }
  });
  var legendItems = [];
  Object.keys(cfg.colors).forEach(function (name) {
    if (present[name]) legendItems.push({ label: name, color: cfg.colors[name] });
  });
  var hasOther = Object.keys(present).some(function (name) {
    return !Object.prototype.hasOwnProperty.call(cfg.colors, name);
  });
  if (cfg.legend && hasOther) {
    legendItems.push({ label: cfg.otherLabel, color: cfg.defaultColor });
  }
  if (!cfg.legend) legendItems = [];

  var legendHeight = 0;
  if (legendItems.length) {
    var lineWidth = 0;
    var lineCount = 1;
    legendItems.forEach(function (item) {
      var w = 12 + 6 + textWidth(item.label.length, 11) + 18;
      if (lineWidth + w > svgWidth - 8) {
        lineCount += 1;
        lineWidth = 0;
      }
      lineWidth += w;
    });
    legendHeight = lineCount * cfg.legendRow + cfg.legendGap;
  }

  var svgHeight = cfg.headerHeight + rowsHeight + legendHeight;

  var svg = d3
    .select(target)
    .append("svg")
    .attr("width", svgWidth)
    .attr("height", svgHeight)
    .attr("role", "img")
    .attr("aria-label", "Hole results by round");

  // --- header -------------------------------------------------------------
  var header = svg.append("g").attr("class", "d3-header");
  header
    .append("text")
    .attr("x", 0)
    .attr("y", cfg.headerHeight - 8)
    .attr("font-size", 11)
    .attr("font-weight", 700)
    .attr("fill", gridHeaderColor)
    .text("Player / Course");

  var metaX = cfg.gutter;
  cfg.meta.forEach(function (m) {
    header
      .append("text")
      .attr("x", metaX)
      .attr("y", cfg.headerHeight - 8)
      .attr("font-size", 11)
      .attr("font-weight", 700)
      .attr("fill", gridHeaderColor)
      .text(m.label);
    metaX += m.width;
  });

  for (var h = 1; h <= holeCount; h += 1) {
    header
      .append("text")
      .attr("x", gridX + (h - 1) * step + cfg.cell / 2)
      .attr("y", cfg.headerHeight - 8)
      .attr("text-anchor", "middle")
      .attr("font-size", 11)
      .attr("font-weight", 700)
      .attr("fill", gridHeaderColor)
      .text(h);
  }

  // --- tooltip ------------------------------------------------------------
  var tip = d3
    .select(target)
    .append("div")
    .attr("class", "d3-heatmap-tip")
    .style("position", "absolute")
    .style("pointer-events", "none")
    .style("opacity", 0)
    .style("z-index", 10)
    .style("transition", "opacity 100ms ease")
    .style("background", dark ? "#111827" : "#FFFFFF")
    .style("color", dark ? "#F3F4F6" : "#111827")
    .style("border", "1px solid " + (dark ? "#374151" : "#D1D5DB"))
    .style("border-radius", "6px")
    .style("padding", "6px 9px")
    .style("font-size", "12px")
    .style("line-height", "1.4")
    .style("box-shadow", "0 4px 12px rgba(0,0,0,.18)")
    .style("max-width", "320px")
    .style("white-space", "nowrap");

  function showTip(event, html) {
    tip
      .html(html)
      .style("left", event.clientX - target.getBoundingClientRect().left + 14 + "px")
      .style(
        "top",
        event.clientY - target.getBoundingClientRect().top + target.scrollTop + 18 + "px"
      )
      .style("opacity", 1);
  }

  function hideTip() {
    tip.style("opacity", 0);
  }

  // --- rows ---------------------------------------------------------------
  var rows = svg
    .selectAll("g.d3-row")
    .data(data)
    .join("g")
    .attr("class", "d3-row")
    .attr("transform", function (d, i) {
      return "translate(0," + (cfg.headerHeight + i * cfg.rowHeight) + ")";
    });

  rows
    .append("text")
    .attr("x", 0)
    .attr("y", 13)
    .attr("font-size", 12)
    .attr("font-weight", 700)
    .attr("fill", labelColor)
    .text(function (d) {
      return clip(d["Player"], Math.floor(cfg.gutter / 6.9));
    });

  rows
    .append("text")
    .attr("x", 0)
    .attr("y", 26)
    .attr("font-size", 11)
    .attr("fill", mutedColor)
    .text(function (d) {
      return clip(d["Course Layout Name"], Math.floor(cfg.gutter / 6.4));
    });

  cfg.meta.forEach(function (m) {
    rows
      .append("text")
      .attr("x", cfg.gutter + cfg.meta
        .slice(0, cfg.meta.indexOf(m))
        .reduce(function (s, x) { return s + x.width; }, 0))
      .attr("y", cfg.rowHeight / 2 + 4)
      .attr("font-size", 11.5)
      .attr("font-weight", m.bold ? 700 : 400)
      .attr("fill", labelColor)
      .text(function (d) {
        var v = d[m.key];
        if (v === null || v === undefined || v === "") return "";
        if (m.key === "UDisc Rating" && isFinite(Number(v))) return String(Math.round(Number(v)));
        return String(v);
      });
  });

  rows.each(function (row) {
    var g = d3.select(this);
    for (var i = 1; i <= holeCount; i += 1) {
      var strokes = row[String(i)];
      if (strokes === null || strokes === undefined || strokes === "") continue;
      var result = row["Result " + i];
      var fill = fillFor(result);
      var x = gridX + (i - 1) * step;
      var cellG = g.append("g");

      cellG
        .append("rect")
        .attr("x", x)
        .attr("y", 4)
        .attr("width", cfg.cell)
        .attr("height", cfg.cell)
        .attr("rx", cfg.radius)
        .attr("ry", cfg.radius)
        .attr("fill", fill);

      cellG
        .append("text")
        .attr("x", x + cfg.cell / 2)
        .attr("y", 4 + cfg.cell / 2 + 4)
        .attr("text-anchor", "middle")
        .attr("font-size", 11)
        .attr("font-weight", 700)
        .attr("fill", luminance(fill) > 0.45 ? "#111827" : "#FFFFFF")
        .attr("pointer-events", "none")
        .text(cellText(strokes));

      cellG
        .append("rect")
        .attr("x", x)
        .attr("y", 4)
        .attr("width", cfg.cell)
        .attr("height", cfg.cell)
        .attr("fill", "transparent")
        .attr("pointer-events", "all")
        .on("mouseenter", function (event) {
          d3.select(this.parentNode).select("rect").attr("stroke", "#FFFFFF").attr("stroke-width", 1.5);
          showTip(
            event,
            "<strong>Hole " +
              i +
              "</strong> · " +
              cellText(strokes) +
              (Number(strokes) === 1 ? " stroke" : " strokes") +
              (result ? " · " + result : "") +
              "<br>" +
              [row["Player"], row["Course Layout Name"], row["Date Formatted"]]
                .filter(function (s) {
                  return s !== null && s !== undefined && s !== "";
                })
                .join(" · ")
          );
        })
        .on("mouseleave", function () {
          d3.select(this.parentNode).select("rect").attr("stroke", null);
          hideTip();
        });
    }
  });

  // --- legend -------------------------------------------------------------
  if (legendItems.length) {
    var legend = svg
      .append("g")
      .attr("class", "d3-legend")
      .attr("transform", "translate(0," + (cfg.headerHeight + rowsHeight + cfg.legendGap) + ")");

    var lx = 0;
    var ly = 0;
    legendItems.forEach(function (item) {
      var w = 12 + 6 + textWidth(item.label.length, 11) + 18;
      if (lx + w > svgWidth - 8) {
        lx = 0;
        ly += cfg.legendRow;
      }
      var g = legend.append("g").attr("transform", "translate(" + lx + "," + ly + ")");
      g.append("rect")
        .attr("width", 12)
        .attr("height", 12)
        .attr("rx", 3)
        .attr("fill", item.color);
      g.append("text")
        .attr("x", 18)
        .attr("y", 10)
        .attr("font-size", 11)
        .attr("fill", mutedColor)
        .text(item.label);
      lx += w;
    });
  }
})();
