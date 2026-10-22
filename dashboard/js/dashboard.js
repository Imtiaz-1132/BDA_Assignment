const DATA_URL = "data/weekly_district_disease.json";
const CENTROIDS_URL = "data/district_centroids.csv";
const GEO_URL = "data/bangladesh_adm2.geojson";

const fmt = d3.format(",");
let allRows = [];
let districtNames = new Map();
let districtIdByName = new Map();
let geoData = null;

function normalizeRow(row) {
  const date = d3.utcParse("%Y-%m-%d")(row.epi_week_start);
  const cases = Number(row.case_count);

  if (!date || !Number.isFinite(cases)) return null;

  return {
    date,
    dateText: row.epi_week_start,
    district: row.district_id,
    disease: row.disease,
    cases,
    patients: Number(row.distinct_patients || 0),
    admissions: Number(row.admission_count || 0),
    deaths: Number(row.death_count || 0),
    labConfirmed: Number(row.lab_confirmed_count || 0)
  };
}

function nameKey(name) {
  return String(name || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function fillFilters() {
  const diseaseSelect = d3.select("#diseaseFilter");
  diseaseSelect.selectAll("option.data-option").remove();

  diseaseSelect.selectAll("option.data-option")
    .data([...new Set(allRows.map(d => d.disease))].sort())
    .join("option")
    .attr("class", "data-option")
    .attr("value", d => d)
    .text(d => d.charAt(0).toUpperCase() + d.slice(1));

  const districtSelect = d3.select("#districtFilter");
  districtSelect.selectAll("option.data-option").remove();

  const districts = [...new Set(allRows.map(d => d.district))]
    .sort((a, b) => districtLabel(a).localeCompare(districtLabel(b)));

  districtSelect.selectAll("option.data-option")
    .data(districts)
    .join("option")
    .attr("class", "data-option")
    .attr("value", d => d)
    .text(d => districtLabel(d));
}

function districtLabel(id) {
  return districtNames.get(id) || id;
}

function filteredRows() {
  const disease = d3.select("#diseaseFilter").property("value");
  const district = d3.select("#districtFilter").property("value");

  return allRows.filter(d =>
    (disease === "all" || d.disease === disease) &&
    (district === "all" || d.district === district)
  );
}

function updateDashboard() {
  const rows = filteredRows();

  d3.select("#totalCases").text(fmt(d3.sum(rows, d => d.cases) || 0));
  d3.select("#districtCount").text(new Set(rows.map(d => d.district)).size);
  d3.select("#diseaseCount").text(new Set(rows.map(d => d.disease)).size);

  drawTrend(rows);
  drawDistricts(rows);
  drawMap(rows);
}

function drawTrend(rows) {
  const root = d3.select("#trendChart").html("");

  if (!rows.length) {
    root.append("p").attr("class", "empty-message")
      .text("No data for these filters.");
    return;
  }

  const values = d3.rollups(
    rows, v => d3.sum(v, d => d.cases), d => +d.date
  ).map(([t, cases]) => ({ date: new Date(t), cases }))
    .sort((a, b) => a.date - b.date);

  const w = 640, h = 300;
  const m = { top: 18, right: 24, bottom: 48, left: 64 };

  const svg = root.append("svg")
    .attr("viewBox", `0 0 ${w} ${h}`)
    .attr("role", "img")
    .attr("aria-label", "Weekly reported case trend");

  const x = d3.scaleUtc()
    .domain(d3.extent(values, d => d.date))
    .range([m.left, w - m.right]);

  const y = d3.scaleLinear()
    .domain([0, (d3.max(values, d => d.cases) || 1) * 1.1])
    .nice()
    .range([h - m.bottom, m.top]);

  svg.append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${h - m.bottom})`)
    .call(d3.axisBottom(x).ticks(6).tickFormat(d3.utcFormat("%Y-%m")));

  svg.append("g")
    .attr("class", "axis")
    .attr("transform", `translate(${m.left},0)`)
    .call(d3.axisLeft(y).ticks(5));

  svg.append("path")
    .datum(values)
    .attr("class", "chart-line")
    .attr("d", d3.line().x(d => x(d.date)).y(d => y(d.cases)));

  svg.selectAll("circle")
    .data(values)
    .join("circle")
    .attr("cx", d => x(d.date))
    .attr("cy", d => y(d.cases))
    .attr("r", 3)
    .attr("fill", "#147d92")
    .append("title")
    .text(d => `${d3.utcFormat("%Y-%m-%d")(d.date)}: ${fmt(d.cases)} cases`);
}

function districtTotals(rows) {
  return d3.rollups(
    rows,
    v => d3.sum(v, d => d.cases),
    d => d.district
  ).map(([district, cases]) => ({
    district,
    name: districtLabel(district),
    cases
  }));
}

function drawDistricts(rows) {
  const root = d3.select("#districtChart").html("");

  const values = districtTotals(rows)
    .sort((a, b) => b.cases - a.cases)
    .slice(0, 12);

  if (!values.length) {
    root.append("p").attr("class", "empty-message")
      .text("No district data for these filters.");
    return;
  }

  const w = 640, h = Math.max(320, values.length * 27 + 60);
  const m = { top: 15, right: 60, bottom: 40, left: 125 };

  const svg = root.append("svg").attr("viewBox", `0 0 ${w} ${h}`);

  const x = d3.scaleLinear()
    .domain([0, d3.max(values, d => d.cases) || 1])
    .nice()
    .range([m.left, w - m.right]);

  const y = d3.scaleBand()
    .domain(values.map(d => d.district))
    .range([m.top, h - m.bottom])
    .padding(0.2);

  svg.append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${h - m.bottom})`)
    .call(d3.axisBottom(x).ticks(5));

  svg.append("g")
    .attr("class", "axis")
    .attr("transform", `translate(${m.left},0)`)
    .call(d3.axisLeft(y).tickFormat(id => districtLabel(id)));

  svg.selectAll("rect")
    .data(values)
    .join("rect")
    .attr("class", "bar")
    .attr("x", m.left)
    .attr("y", d => y(d.district))
    .attr("width", d => x(d.cases) - m.left)
    .attr("height", y.bandwidth())
    .on("click", (_, d) => {
      d3.select("#districtFilter").property("value", d.district);
      updateDashboard();
    })
    .append("title")
    .text(d => `${d.name}: ${fmt(d.cases)} cases. Click to filter.`);

  svg.selectAll("text.bar-value")
    .data(values)
    .join("text")
    .attr("class", "bar-value")
    .attr("x", d => x(d.cases) + 5)
    .attr("y", d => y(d.district) + y.bandwidth() / 2)
    .attr("dy", "0.35em")
    .attr("font-size", 11)
    .text(d => fmt(d.cases));
}

function ensureMapPanel() {
  if (document.querySelector("#mapChart")) return;

  const charts = document.querySelector(".charts");
  if (!charts) return;

  const panel = document.createElement("article");
  panel.className = "panel map-panel";
  panel.innerHTML =
    '<h2>Bangladesh district case map</h2>' +
    '<p class="map-help">Darker districts have more reported cases under the current filters. Hover over a district for details; click to filter.</p>' +
    '<div id="mapChart"></div>';

  charts.appendChild(panel);
}

function drawMap(rows) {
  const root = d3.select("#mapChart");
  if (root.empty() || !geoData) return;

  root.html("");

  const totals = new Map(
    districtTotals(rows).map(d => [d.district, d.cases])
  );

  const features = geoData.features || [];
  const matched = features.filter(feature =>
    districtIdByName.has(nameKey(feature.properties?.shapeName))
  );

  if (!matched.length) {
    root.append("p").attr("class", "empty-message")
      .text("Could not match GeoJSON district names to the district registry.");
    return;
  }

  const w = 640, h = 620;
  const svg = root.append("svg")
    .attr("viewBox", `0 0 ${w} ${h}`)
    .attr("role", "img")
    .attr("aria-label", "Map of Bangladesh district case counts");

  const collection = { type: "FeatureCollection", features: matched };
  const projection = d3.geoMercator().fitExtent(
    [[18, 18], [w - 18, h - 18]], collection
  );
  const path = d3.geoPath(projection);

  const maxCases = d3.max(matched, feature => {
    const id = districtIdByName.get(nameKey(feature.properties?.shapeName));
    return totals.get(id) || 0;
  }) || 1;

  const color = d3.scaleSequential()
    .domain([0, maxCases])
    .interpolator(d3.interpolateBlues);

  svg.selectAll("path.district")
    .data(matched)
    .join("path")
    .attr("class", "district")
    .attr("d", path)
    .attr("fill", feature => {
      const id = districtIdByName.get(nameKey(feature.properties?.shapeName));
      return color(totals.get(id) || 0);
    })
    .on("click", (_, feature) => {
      const id = districtIdByName.get(nameKey(feature.properties?.shapeName));
      if (id) {
        d3.select("#districtFilter").property("value", id);
        updateDashboard();
      }
    })
    .append("title")
    .text(feature => {
      const name = feature.properties?.shapeName || "Unknown district";
      const id = districtIdByName.get(nameKey(name));
      return `${name}: ${fmt(totals.get(id) || 0)} reported cases`;
    });

  const legendWidth = 220;
  const legend = svg.append("g")
    .attr("transform", `translate(${w - legendWidth - 24},${h - 32})`);

  const gradientId = "case-map-gradient";
  const defs = svg.append("defs");
  const gradient = defs.append("linearGradient").attr("id", gradientId);

  gradient.append("stop").attr("offset", "0%").attr("stop-color", color(0));
  gradient.append("stop").attr("offset", "100%").attr("stop-color", color(maxCases));

  legend.append("rect")
    .attr("width", legendWidth)
    .attr("height", 10)
    .attr("fill", `url(#${gradientId})`);

  legend.append("text").attr("y", -5).attr("font-size", 11)
    .text("Reported cases");

  legend.append("text").attr("y", 25).attr("font-size", 10)
    .text("0");

  legend.append("text")
    .attr("x", legendWidth)
    .attr("y", 25)
    .attr("text-anchor", "end")
    .attr("font-size", 10)
    .text(fmt(maxCases));

  root.append("p")
    .attr("class", "map-help")
    .text(`Matched ${matched.length} of ${features.length} GeoJSON districts.`);
}

async function initDashboard() {
  try {
    ensureMapPanel();

    const [raw, centroids, geo] = await Promise.all([
      d3.json(DATA_URL),
      d3.csv(CENTROIDS_URL),
      d3.json(GEO_URL)
    ]);

    allRows = raw.map(normalizeRow).filter(Boolean);
    geoData = geo;

    centroids.forEach(row => {
      districtNames.set(row.district_id, row.district_name);
      districtIdByName.set(nameKey(row.district_name), row.district_id);
    });

    fillFilters();

    d3.select("#dataStatus")
      .text(`Loaded ${fmt(allRows.length)} aggregated Gold records.`);

    d3.select("#diseaseFilter").on("change", updateDashboard);
    d3.select("#districtFilter").on("change", updateDashboard);

    d3.select("#resetButton").on("click", () => {
      d3.select("#diseaseFilter").property("value", "all");
      d3.select("#districtFilter").property("value", "all");
      updateDashboard();
    });

    updateDashboard();
  } catch (error) {
    console.error("Dashboard initialization failed:", error);
    d3.select("#dataStatus")
      .text(`Dashboard data unavailable: ${error.message}`);
    d3.select("#trendChart").text("Could not load Gold data.");
    d3.select("#districtChart").text("Could not load district data.");
    d3.select("#mapChart").text("Could not load district map data.");
  }
}

initDashboard();

