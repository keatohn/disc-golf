/* Score distribution histogram: one bar per score-vs-par value.
 *
 * Available in scope (injected by components/d3_render.py):
 *   target  -- mount element (must be cleared before rendering)
 *   payload -- {data: [round records], config: {...}}
 *   d3      -- D3 v7 global
 *
 * Record shape (from analytics.rounds):
 *   "Score" (int, negative = under par), "Score Display", "Player",
 *   "Course Layout Name", "Date Formatted".
 */
(function () {
  var cfg = Object.assign(
    {
      underColor: "#4C9BE8",
      parColor: "#D3D3D3",
      overColor: "#E8A14C",
      margin: { top: 16, right: 16, bottom: 34, left: 40 },
      valueKey: "Score",
      meanLine: true,
      minBarWidth: 14
    },
    payload.config || {}
  );

  var data = (payload && payload.data) || [];

  while (target.firstChild) target.removeChild(target.firstChild);
  target.style.position = "relative";
  target.style.overflow = "hidden";

  if (!data.length) {
    var empty = document.createElement("div");
    empty.style.cssText = "padding:12px;font-size:13px;opacity:.7;";
    empty.textContent = "No rounds to display";
    target.appendChild(empty);
    return;
  }

  var dark =
    cfg.theme && typeof cfg.theme.dark === "boolean"
      ? cfg.theme.dark
      : !!(window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
  var mutedColor = dark ? "#9CA3AF" : "#5B6470";
  var axisColor = dark ? "#4B5563" : "#9AA3AD";

  var scores = data
    .map(function (d) { return Number(d[cfg.valueKey]); })
    .filter(function (v) { return isFinite(v); });

  if (!scores.length) {
    var noScore = document.createElement("div");
    noScore.style.cssText = "padding:12px;font-size:13px;opacity:.7;";
    noScore.textContent = "No scores to display";
    target.appendChild(noScore);
    return;
  }

  var total = scores.length;
  var mean = d3.mean(scores);

  // Bin every integer score between observed min and max (inclusive) so
  // gaps (e.g. no +9 rounds) still show as empty slots on the axis.
  var minScore = d3.min(scores);
  var maxScore = d3.max(scores);
  var bins = d3.range(minScore, maxScore + 1).map(function (score) {
    return { score: score, count: 0 };
  });
  var byScore = {};
  bins.forEach(function (b) { byScore[b.score] = b; });
  scores.forEach(function (s) {
    var b = byScore[s];
    if (b) b.count += 1;
  });

  var width = Math.max(target.clientWidth || 640, bins.length * cfg.minBarWidth);
  var height = target.clientHeight || 220;
  var innerW = width - cfg.margin.left - cfg.margin.right;
  var innerH = height - cfg.margin.top - cfg.margin.bottom;

  var x = d3
    .scaleBand()
    .domain(bins.map(function (b) { return b.score; }))
    .range([0, innerW])
    .padding(0.18);
  var y = d3
    .scaleLinear()
    .domain([0, d3.max(bins, function (b) { return b.count; }) || 1])
    .nice()
    .range([innerH, 0]);

  function fillFor(score) {
    if (score < 0) return cfg.underColor;
    if (score === 0) return cfg.parColor;
    return cfg.overColor;
  }

  var svg = d3
    .select(target)
    .append("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("role", "img")
    .attr("aria-label", "Distribution of rounds by score versus par");

  var g = svg
    .append("g")
    .attr("transform", "translate(" + cfg.margin.left + "," + cfg.margin.top + ")");

  // --- tooltip ------------------------------------------------------------
  var tip = d3
    .select(target)
    .append("div")
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
    .style("white-space", "nowrap");

  function showTip(event, html) {
    tip
      .html(html)
      .style("left", event.clientX - target.getBoundingClientRect().left + 14 + "px")
      .style("top", event.clientY - target.getBoundingClientRect().top + 12 + "px")
      .style("opacity", 1);
  }

  function hideTip() {
    tip.style("opacity", 0);
  }

  function scoreLabel(score) {
    if (score < 0) return String(score);
    if (score === 0) return "E";
    return "+" + score;
  }

  // --- axes ---------------------------------------------------------------
  g.append("g")
    .attr("class", "d3-x-axis")
    .attr("transform", "translate(0," + innerH + ")")
    .call(d3.axisBottom(x).tickFormat(scoreLabel).tickSize(0))
    .call(function (s) { s.select(".domain").attr("stroke", axisColor); })
    .call(function (s) {
      s.selectAll("text")
        .attr("fill", mutedColor)
        .attr("font-size", 11)
        .attr("dy", "0.9em");
    });

  g.append("g")
    .attr("class", "d3-y-axis")
    .call(
      d3.axisLeft(y).ticks(4).tickSize(-innerW).tickFormat(d3.format("d"))
    )
    .call(function (s) { s.select(".domain").remove(); })
    .call(function (s) {
      s.selectAll("line")
        .attr("stroke", dark ? "#2A3140" : "#E5E7EB");
      s.selectAll("text")
        .attr("fill", mutedColor)
        .attr("font-size", 11);
    });

  // --- bars ---------------------------------------------------------------
  g.selectAll("rect.d3-bar")
    .data(bins)
    .join("rect")
    .attr("class", "d3-bar")
    .attr("x", function (d) { return x(d.score); })
    .attr("width", x.bandwidth())
    .attr("y", innerH)
    .attr("height", 0)
    .attr("rx", 3)
    .attr("fill", function (d) { return fillFor(d.score); })
    .style("cursor", "default")
    .on("mouseenter", function (event, d) {
      d3.select(this).attr("opacity", 0.8);
      var pct = (d.count / total) * 100;
      showTip(
        event,
        "<strong>" + scoreLabel(d.score) + "</strong> · " +
          d.count + (d.count === 1 ? " round" : " rounds") +
          " · " + pct.toFixed(1) + "%"
      );
    })
    .on("mouseleave", function () {
      d3.select(this).attr("opacity", 1);
      hideTip();
    })
    .transition()
    .duration(350)
    .attr("y", function (d) { return y(d.count); })
    .attr("height", function (d) { return innerH - y(d.count); });

  // --- mean marker --------------------------------------------------------
  if (cfg.meanLine && isFinite(mean)) {
    // Interpolate between band centers for a sub-bin-precision mean position.
    var first = x(minScore);
    var last = x(maxScore);
    var meanX =
      first +
      ((mean - minScore) / (maxScore - minScore || 1)) * (last - first) +
      x.bandwidth() / 2;

    var meanG = g.append("g").attr("class", "d3-mean").attr("pointer-events", "none");
    meanG
      .append("line")
      .attr("x1", meanX)
      .attr("x2", meanX)
      .attr("y1", -cfg.margin.top + 4)
      .attr("y2", innerH)
      .attr("stroke", dark ? "#F3F4F6" : "#111827")
      .attr("stroke-width", 1.5)
      .attr("stroke-dasharray", "4,3");
    meanG
      .append("text")
      .attr("x", meanX)
      .attr("y", -2)
      .attr("text-anchor", "middle")
      .attr("font-size", 10)
      .attr("fill", mutedColor)
      .text("avg " + mean.toFixed(1));
  }
})();
