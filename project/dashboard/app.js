const numberFmt = new Intl.NumberFormat("en-US");
const percentFmt = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 1,
});

function byId(id) {
  return document.getElementById(id);
}

function setText(id, value) {
  byId(id).textContent = value;
}

function formatNumber(value) {
  return numberFmt.format(Number(value || 0));
}

function formatPercent(value) {
  return `${percentFmt.format(Number(value || 0))}%`;
}

function formatSeconds(value) {
  const seconds = Number(value || 0);
  const minutes = Math.floor(seconds / 60);
  const rest = Math.round(seconds % 60);
  return `${minutes}m ${rest}s`;
}

function rowsFromObject(obj) {
  return Object.entries(obj || {}).map(([label, value]) => ({ label, value }));
}

function renderBars(id, rows, options = {}) {
  const root = byId(id);
  root.innerHTML = "";
  const data = rows || [];
  const max = Math.max(...data.map((row) => Number(row.value || 0)), 1);

  data.forEach((row) => {
    const item = document.createElement("div");
    item.className = "bar-row";

    const label = document.createElement("div");
    label.className = "bar-label";
    label.title = row.label;
    label.textContent = row.label;

    const track = document.createElement("div");
    track.className = "bar-track";

    const fill = document.createElement("div");
    fill.className = `bar-fill ${options.tone || ""}`.trim();
    fill.style.width = `${Math.max((Number(row.value || 0) / max) * 100, 2)}%`;
    track.appendChild(fill);

    const value = document.createElement("div");
    value.className = "bar-value";
    value.textContent = options.format ? options.format(row.value) : formatNumber(row.value);

    item.append(label, track, value);
    root.appendChild(item);
  });
}

function renderTable(id, rows, columns) {
  const root = byId(id);
  root.innerHTML = "";

  (rows || []).forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      const raw = row[column.key];
      td.textContent = column.format ? column.format(raw, row) : raw;
      tr.appendChild(td);
    });
    root.appendChild(tr);
  });
}

function renderInsights(insights) {
  const root = byId("insight-strip");
  root.innerHTML = "";
  (insights || []).forEach((insight) => {
    const article = document.createElement("article");
    article.className = "insight";
    article.innerHTML = `
      <span>${insight.label}</span>
      <strong>${insight.value}</strong>
      <p>${insight.detail}</p>
    `;
    root.appendChild(article);
  });
}

function renderProfile(profileRows) {
  const root = byId("dataset-profile");
  root.innerHTML = "";
  const metrics = [
    { key: "row_count", label: "Rows", format: formatNumber },
    { key: "avg_duration_seconds", label: "Avg duration", format: formatSeconds },
    { key: "avg_word_count", label: "Avg words", format: (v) => formatNumber(Math.round(v || 0)) },
    { key: "avg_char_count", label: "Avg chars", format: (v) => formatNumber(Math.round(v || 0)) },
    { key: "avg_pii_token_count", label: "PII tokens", format: (v) => v == null ? "N/A" : Number(v).toFixed(1) },
  ];

  (profileRows || []).forEach((dataset) => {
    const block = document.createElement("div");
    block.className = "profile-block";
    const title = document.createElement("h3");
    title.textContent = dataset.dataset_label || dataset.dataset_name;
    const role = document.createElement("p");
    role.textContent = dataset.role || "";
    const grid = document.createElement("div");
    grid.className = "profile-grid";

    metrics.forEach((metric) => {
      const item = document.createElement("div");
      item.innerHTML = `<span>${metric.label}</span><strong>${metric.format(dataset[metric.key])}</strong>`;
      grid.appendChild(item);
    });

    block.append(title, role, grid);
    root.appendChild(block);
  });
}

function renderCallCodeShift(primaryRows, callcenterRows) {
  const root = byId("call-code-shift");
  root.innerHTML = "";
  [
    { title: "Primary AGI Telesales", rows: primaryRows || [], tone: "blue" },
    { title: "CallCenterEN model labels", rows: callcenterRows || [], tone: "teal" },
  ].forEach((group) => {
    const block = document.createElement("div");
    block.className = "split-block";
    const title = document.createElement("h3");
    title.textContent = group.title;
    const list = document.createElement("div");
    list.className = "bars compact";
    block.append(title, list);
    root.appendChild(block);
    renderBarsInto(list, group.rows.slice(0, 8), group.tone);
  });
}

function renderBarsInto(root, rows, tone) {
  const max = Math.max(...rows.map((row) => Number(row.value || 0)), 1);
  rows.forEach((row) => {
    const item = document.createElement("div");
    item.className = "bar-row";
    item.innerHTML = `
      <div class="bar-label" title="${row.label}">${row.label}</div>
      <div class="bar-track"><div class="bar-fill ${tone}" style="width:${Math.max((Number(row.value || 0) / max) * 100, 2)}%"></div></div>
      <div class="bar-value">${formatNumber(row.value)}</div>
    `;
    root.appendChild(item);
  });
}

async function loadDashboard() {
  const statusDot = byId("status-dot");
  const emptyState = byId("empty-state");

  try {
    const response = await fetch("dashboard_data.json", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`dashboard_data.json returned ${response.status}`);
    }

    const data = await response.json();
    const kpis = data.kpis;
    const comparison = data.comparison || {};
    const callcenter = data.callcenteren || {};

    setText("kpi-primary-calls", formatNumber(kpis.total_calls));
    setText("kpi-callcenter-calls", formatNumber(kpis.callcenteren_calls));
    setText("kpi-serving-rows", formatNumber(kpis.total_serving_rows));
    setText("kpi-success-rate", formatPercent(kpis.success_rate));
    setText("kpi-duration-gap", `+${Math.round(kpis.duration_gap_seconds || 0)}s`);
    setText("kpi-code-links", formatNumber(kpis.callcenteren_bridge_rows));

    renderInsights(data.insights);
    renderProfile(comparison.dataset_profiles);
    renderBars("outcome-category", data.charts.outcome_category);
    renderBars("product-category", data.charts.product_category, { tone: "amber" });
    renderBars("callcenter-domains", rowsFromObject(callcenter.domain_distribution), { tone: "teal" });
    renderBars("callcenter-direction", rowsFromObject(callcenter.direction_distribution), { tone: "green" });
    renderCallCodeShift(
      comparison.primary_top_call_codes,
      callcenter.top_model_call_codes,
    );

    renderTable("model-evidence", data.models, [
      { key: "model_label" },
      { key: "eval_dataset" },
      { key: "eval_rows", format: formatNumber },
      { key: "micro_f1", format: (value) => formatPercent(Number(value) * 100) },
      { key: "exact_match_rate", format: (value) => formatPercent(Number(value) * 100) },
    ]);

    renderTable("pipeline-evidence", data.evidence, [
      { key: "layer" },
      { key: "object" },
      { key: "rows", format: formatNumber },
      { key: "status" },
    ]);

    statusDot.className = "dot ready";
    setText("generated-at", `Updated ${new Date(data.generated_at).toLocaleString()}`);
    emptyState.hidden = true;
  } catch (error) {
    statusDot.className = "dot error";
    setText("generated-at", "Dashboard data is not ready");
    emptyState.hidden = false;
    console.error(error);
  }
}

loadDashboard();
