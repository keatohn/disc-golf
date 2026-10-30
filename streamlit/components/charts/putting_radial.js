/* Putting radial: C1 / C1X / C2 make% drawn as radial bars around a circle.
 *
 * Available in scope (injected by components/d3_render.py):
 *   target  -- mount element (must be cleared before rendering)
 *   payload -- {data: [zone records], config: {...}}
 *   d3      -- D3 v7 global
 *
 * Record shape (summed in Python from the filtered analytics.rounds rows):
 *   {zone: "C1" | "C1X" | "C2", made: int, attempted: int}
 * Percentages are computed here for display only.
 */
(function () {
  var cfg = Object.assign(
    {
      zoneColors: { C1: "#22C55E", C1X: "#4C9BE8", C2: "#E8A14C" },
      zoneHints: {
        C1: "putts inside 10 m",
        C1X: "C1 excluding tap-ins",
        C2: "putts from 10-20 m"
      },
      ringPcts: [25, 50, 75, 100],
      padAngle: 0.012,
      cornerRadius: 3,
      innerRatio: 0.46,
      labelPad: 60
    },
    payload.config || {}
  );

  var data = (payload && payload.data) || [];

  while (target.firstChild) target.removeChild(target.firstChild);
  target.style.position = "relative";
  target.style.overflow = "hidden";

  function message(text) {
    var div = document.createElement("div");
    div.style.cssText = "padding:12px;font-size:13px;opacity:.7;";
    div.textContent = text;
    target.appendChild(div);
  }

  var dark =
    cfg.theme && typeof cfg.theme.dark === "boolean"
      ? cfg.theme.dark
      : !!(window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
  var mutedColor = dark ? "#9CA3AF" : "#5B6470";
  var axisColor = dark ? "#4B5563" : "#9AA3AD";
  var textColor = dark ? "#E5E7EB" : "#374151";
  var trackColor = dark ? "#252B36" : "#E8ECF2";

  var zones = data
    .map(function (d) {
      var attempted = Number(d.attempted);
      var made = Number(d.made);
      if (!isFinite(attempted)) attempted = 0;
      if (!isFinite(made)) made = 0;
      var zone = d.zone == null ? "" : String(d.zone);
      return {
        zone: zone,
        made: made,
        attempted: attempted,
        pct: attempted > 0 ? (made / attempted) * 100 : 0,
        hasData: attempted > 0
      };
    })
    .filter(function (d) { return d.zone.length > 0; });

  if (!zones.length) {
    message("No putting data to display");
    return;
  }
  if (!zones.some(function (d) { return d.hasData; })) {
    message("No putts recorded in these rounds");
    return;
  }

  var width = target.clientWidth || 480;
  var height = target.clientHeight || 340;
  var cx = width / 2;
  var cy = height / 2;
  var R = Math.max(48, Math.min(width, height) / 2 - cfg.labelPad);
  var innerR = R * cfg.innerRatio;

  function rScale(pct) {
    var p = Math.max(0, Math.min(100, pct));
    return innerR + (R - innerR) * (p / 100);
  }

  // Equal sweep per zone, first zone starting at 12 o'clock, going clockwise.
  var sweep = (2 * Math.PI) / zones.length;
  zones.forEach(function (z, i) {
    z.startAngle = -Math.PI / 2 + i * sweep;
    z.endAngle = z.startAngle + sweep;
    z.midAngle = z.startAngle + sweep / 2;
    z.color = cfg.zoneColors[z.zone] || mutedColor;
  });

  function arcPath(z, outerR) {
    return d3
      .arc()
      .innerRadius(innerR)
      .outerRadius(outerR)
      .padAngle(cfg.padAngle)
      .cornerRadius(cfg.cornerRadius)({
        startAngle: z.startAngle,
        endAngle: z.endAngle
      });
  }

  var svg = d3
    .select(target)
    .append("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("role", "img")
    .attr("aria-label", "Putting radial chart: make percentage by putting zone");

  var g = svg
    .append("g")
    .attr("transform", "translate(" + cx + "," + cy + ")");

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

  function tipHtml(z) {
    var hint = cfg.zoneHints[z.zone];
    var hintLine = hint
      ? '<div style="color:' + mutedColor + ';font-size:11px">' + hint + "</div>"
      : "";
    return (
      "<strong>" + z.zone + " " + z.pct.toFixed(1) + "%</strong><br>" +
      z.made + " of " + z.attempted + " made" + hintLine
    );
  }

  // --- tracks (full radius per zone) --------------------------------------
  g.selectAll("path.d3-track")
    .data(zones)
    .join("path")
    .attr("class", "d3-track")
    .attr("fill", trackColor)
    .attr("d", function (z) { return arcPath(z, R); });

  // --- percentage rings (under the value arcs) ----------------------------
  var rings = g
    .append("g")
    .attr("class", "d3-rings")
    .attr("pointer-events", "none");
  cfg.ringPcts.forEach(function (pct) {
    rings
      .append("circle")
      .attr("r", rScale(pct))
      .attr("fill", "none")
      .attr("stroke", axisColor)
      .attr("stroke-width", 1)
      .attr("stroke-dasharray", pct >= 100 ? null : "3,3")
      .attr("opacity", 0.85);
  });

  // --- value arcs (grow from the inner radius) ----------------------------
  g.selectAll("path.d3-value")
    .data(zones.filter(function (z) { return z.hasData; }))
    .join("path")
    .attr("class", "d3-value")
    .attr("fill", function (z) { return z.color; })
    .attr("d", function (z) { return arcPath(z, innerR); })
    .style("cursor", "default")
    .on("mouseenter", function (event, z) {
      d3.select(this).attr("opacity", 0.85);
      showTip(event, tipHtml(z));
    })
    .on("mouseleave", function () {
      d3.select(this).attr("opacity", 1);
      hideTip();
    })
    .transition()
    .duration(350)
    .attrTween("d", function (z) {
      var i = d3.interpolate(0, z.pct);
      return function (t) { return arcPath(z, rScale(i(t))); };
    });

  // --- zone labels around the outside -------------------------------------
  var labels = g
    .append("g")
    .attr("class", "d3-zone-labels")
    .attr("pointer-events", "none");

  zones.forEach(function (z) {
    var lx = Math.cos(z.midAngle);
    var ly = Math.sin(z.midAngle);
    var anchor = lx > 0.2 ? "start" : lx < -0.2 ? "end" : "middle";

    labels
      .append("text")
      .attr("x", (R + 17) * lx)
      .attr("y", (R + 17) * ly)
      .attr("text-anchor", anchor)
      .attr("dominant-baseline", "middle")
      .attr("font-size", 12)
      .attr("font-weight", 600)
      .attr("fill", z.hasData ? z.color : mutedColor)
      .text(z.zone);

    labels
      .append("text")
      .attr("x", (R + 31) * lx)
      .attr("y", (R + 31) * ly)
      .attr("text-anchor", anchor)
      .attr("dominant-baseline", "middle")
      .attr("font-size", 11)
      .attr("fill", z.hasData ? textColor : mutedColor)
      .text(z.hasData ? z.pct.toFixed(1) + "%" : "no putts");

    if (z.hasData) {
      labels
        .append("text")
        .attr("x", (R + 44) * lx)
        .attr("y", (R + 44) * ly)
        .attr("text-anchor", anchor)
        .attr("dominant-baseline", "middle")
        .attr("font-size", 10)
        .attr("fill", mutedColor)
        .text(z.made + "/" + z.attempted);
    }
  });

  // --- center: unweighted average of the zone make% -----------------------
  var withData = zones.filter(function (z) { return z.hasData; });
  var avg = d3.mean(withData, function (z) { return z.pct; });

  var center = g
    .append("g")
    .attr("class", "d3-center")
    .attr("pointer-events", "none");
  center
    .append("text")
    .attr("x", 0)
    .attr("y", -2)
    .attr("text-anchor", "middle")
    .attr("dominant-baseline", "middle")
    .attr("font-size", 22)
    .attr("font-weight", 700)
    .attr("fill", textColor)
    .text(isFinite(avg) ? avg.toFixed(1) + "%" : "-");
  center
    .append("text")
    .attr("x", 0)
    .attr("y", 16)
    .attr("text-anchor", "middle")
    .attr("dominant-baseline", "middle")
    .attr("font-size", 10)
    .attr("fill", mutedColor)
    .text("avg make%");

  // --- ring scale note -----------------------------------------------------
  svg
    .append("text")
    .attr("x", 8)
    .attr("y", 15)
    .attr("font-size", 10)
    .attr("fill", mutedColor)
    .style("pointer-events", "none")
    .text("rings = " + cfg.ringPcts.join(" / ") + "% made");
})();
