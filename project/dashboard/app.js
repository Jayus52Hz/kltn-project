const numberFmt = new Intl.NumberFormat("en-US");

function setText(id, value) {
  document.getElementById(id).textContent = value;
}

function formatSeconds(value) {
  const seconds = Number(value || 0);
  const minutes = Math.floor(seconds / 60);
  const rest = Math.round(seconds % 60);
  return `${minutes}m ${rest}s`;
}

function renderBars(id, rows) {
  const root = document.getElementById(id);
  root.innerHTML = "";
  const max = Math.max(...rows.map((row) => row.value), 1);

  rows.forEach((row) => {
    const item = document.createElement("div");
    item.className = "bar-row";

    const label = document.createElement("div");
    label.className = "bar-label";
    label.title = row.label;
    label.textContent = row.label;

    const track = document.createElement("div");
    track.className = "bar-track";

    const fill = document.createElement("div");
    fill.className = "bar-fill";
    fill.style.width = `${Math.max((row.value / max) * 100, 2)}%`;
    track.appendChild(fill);

    const value = document.createElement("div");
    value.className = "bar-value";
    value.textContent = numberFmt.format(row.value);

    item.append(label, track, value);
    root.appendChild(item);
  });
}

function renderTable(id, rows, columns) {
  const root = document.getElementById(id);
  root.innerHTML = "";

  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      const raw = row[column.key];
      td.textContent = column.format ? column.format(raw) : raw;
      tr.appendChild(td);
    });
    root.appendChild(tr);
  });
}

async function loadDashboard() {
  const statusDot = document.getElementById("status-dot");
  const emptyState = document.getElementById("empty-state");

  try {
    const response = await fetch("dashboard_data.json", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`dashboard_data.json returned ${response.status}`);
    }

    const data = await response.json();
    const kpis = data.kpis;

    setText("kpi-total-calls", numberFmt.format(kpis.total_calls));
    setText("kpi-success-rate", `${kpis.success_rate}%`);
    setText("kpi-sales", numberFmt.format(kpis.successful_sales));
    setText("kpi-talk-time", formatSeconds(kpis.avg_talk_time_seconds));
    setText("kpi-customers", numberFmt.format(kpis.total_customers));
    setText("kpi-offers", numberFmt.format(kpis.total_offers));

    renderBars("daily-calls", data.charts.daily_calls);
    renderBars("outcome-category", data.charts.outcome_category);
    renderBars("call-status", data.charts.call_status);
    renderBars("product-category", data.charts.product_category);
    renderBars("talk-time-band", data.charts.talk_time_band);
    renderBars("credit-tier", data.charts.credit_tier);

    renderTable("top-campaigns", data.tables.top_campaigns, [
      { key: "campaign_id" },
      { key: "calls", format: numberFmt.format },
      { key: "sales", format: numberFmt.format },
      { key: "avg_talk_time_seconds", format: formatSeconds },
    ]);

    renderTable("top-products", data.tables.top_products, [
      { key: "product_name" },
      { key: "calls", format: numberFmt.format },
      { key: "sales", format: numberFmt.format },
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
