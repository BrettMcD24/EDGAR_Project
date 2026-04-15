/**
 * app.js — EDGAR Financial Viewer (Flask edition)
 *
 * The frontend no longer talks to SEC EDGAR directly.
 * It calls /api/financials on the Flask server, which handles all
 * EDGAR requests server-side and returns clean JSON.
 *
 * This file is responsible for:
 *   - UI controls (filing type, periods, tabs)
 *   - Calling the Flask API
 *   - Rendering the returned JSON into tables
 *   - CSV download
 */

// ── App state ──────────────────────────────────────────────────────────────
const state = {
  filing:  '10-K',
  periods: 5,
  data:    null,   // last successful API response
};

// ── DOM helper ─────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);

// ── Hero search ───────────────────────────────────────────────────────────────
function heroSearch() {
  const hero = document.getElementById('heroTickerInput');
  const ticker = hero ? hero.value.trim().toUpperCase() : '';
  if (!ticker) return;
  document.getElementById('tickerInput').value = ticker;
  document.getElementById('search-section').scrollIntoView({ behavior: 'smooth' });
  setTimeout(loadTicker, 420);
}
document.addEventListener('DOMContentLoaded', () => {
  const hero = document.getElementById('heroTickerInput');
  if (hero) hero.addEventListener('keydown', e => { if (e.key === 'Enter') heroSearch(); });
});

// ── Controls ───────────────────────────────────────────────────────────────
function setFiling(el) {
  document.querySelectorAll('[data-filing]').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  state.filing = el.dataset.filing;
}

function setPeriods(el) {
  document.querySelectorAll('[data-n]').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  state.periods = parseInt(el.dataset.n);
}

function switchTab(el) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  document.querySelectorAll('.stmt-panel').forEach(p => p.classList.remove('active'));
  $(`${el.dataset.tab}Panel`).classList.add('active');
}

function quickLoad(ticker) {
  $('tickerInput').value = ticker;
  loadTicker();
}

$('tickerInput').addEventListener('keydown', e => {
  if (e.key === 'Enter') loadTicker();
});

// ── Status helpers ─────────────────────────────────────────────────────────
function showStatus(msg, type = 'loading') {
  const el = $('status');
  el.className = type;
  $('statusMsg').textContent = msg;
  $('spinner').style.display = type === 'loading' ? 'block' : 'none';
}
function hideStatus() {
  $('status').className = '';
  $('status').style.display = '';
}
function showError(msg) { showStatus(msg, 'error'); }

// ── Main: call Flask API ───────────────────────────────────────────────────
async function loadTicker() {
  const ticker = $('tickerInput').value.trim().toUpperCase();
  if (!ticker) return;

  $('loadBtn').disabled = true;
  $('emptyState').style.display    = 'none';
  $('companyBanner').style.display = 'none';
  $('stmtTabs').style.display      = 'none';
  $('tableArea').style.display     = 'none';

  try {
    showStatus(`Loading ${ticker}…`);

    const url  = `/api/financials?ticker=${ticker}&filing=${state.filing}&periods=${state.periods}`;
    const resp = await fetch(url);
    const json = await resp.json();

    if (!resp.ok) {
      throw new Error(json.error || `Server error ${resp.status}`);
    }

    state.data = json;
    hideStatus();
    renderAll(json);

  } catch (e) {
    showError(e.message || 'Unexpected error. Check the Flask console.');
    $('emptyState').style.display = 'block';
  } finally {
    $('loadBtn').disabled = false;
  }
}

// ── Rendering ──────────────────────────────────────────────────────────────

// Section break labels for visual grouping in each statement
const SECTION_BREAKS = {
  income_statement: {
    'Cost of Revenue':           'Cost & Gross Margin',
    'R&D Expense':               'Operating Expenses',
    'Operating Income (Loss)':   'Operating Result',
    'Interest Expense':          'Non-Operating Items',
    'Pre-Tax Income (Loss)':     'Tax & Net Income',
    'EPS – Basic':               'Per Share Data',
  },
  balance_sheet: {
    'Total Current Assets':      'Non-Current Assets',
    'Total Assets':              'Current Liabilities',
    'Total Current Liabilities': 'Non-Current Liabilities',
    'Total Liabilities':         'Equity',
  },
  cash_flow: {
    'Net Cash – Investing': 'Investing Activities',
    'Net Cash – Financing': 'Financing Activities',
    'Net Change in Cash':   'Summary',
  },
};

function renderAll(data) {
  const { ticker, company, cik, filing, periods, labels, statements, ratios } = data;

  // Banner
  $('bannerTicker').textContent       = ticker;
  $('bannerName').textContent         = company;
  $('bannerSub').textContent          = `CIK ${cik}  ·  ${filing}  ·  Source: SEC EDGAR`;
  $('bannerPeriodsCount').textContent = periods.length;
  $('bannerPeriodsLabels').textContent = labels.join('  ·  ');
  $('companyBanner').style.display    = 'flex';

  $('stmtTabs').style.display  = 'flex';
  $('tableArea').style.display = 'block';

  // Reset to first tab
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === 'income'));
  document.querySelectorAll('.stmt-panel').forEach(p => p.classList.toggle('active', p.id === 'incomePanel'));

  renderPanel('incomePanel',   statements.income_statement, periods, labels, 'income_statement');
  renderPanel('balancePanel',  statements.balance_sheet,    periods, labels, 'balance_sheet');
  renderPanel('cashflowPanel', statements.cash_flow,        periods, labels, 'cash_flow');
  renderRatiosPanel('ratiosPanel', ratios, periods, labels);
}

function renderPanel(panelId, rows, periods, labels, stmtKey) {
  const panel  = $(panelId);
  const breaks = SECTION_BREAKS[stmtKey] || {};

  if (!rows || !rows.length) {
    panel.innerHTML = '<p class="no-data">No data available for this statement.</p>';
    return;
  }

  const colSpan = periods.length + 1;
  let html = buildMeta(stmtKey);
  html += `<div class="table-scroll"><table class="fin-table">`;
  html += buildHeader(labels);
  html += `<tbody>`;

  for (const row of rows) {
    if (breaks[row.label]) {
      html += `<tr class="section-row"><td colspan="${colSpan}">${breaks[row.label]}</td></tr>`;
    }
    html += `<tr><td>${row.label}</td>`;
    periods.forEach((p, i) => {
      const v   = row.values?.[p];
      const cls = [i === 0 ? 'col-latest' : '', valClass(v)].filter(Boolean).join(' ');
      html += `<td class="${cls}">${fmt(v)}</td>`;
    });
    html += `</tr>`;
  }

  html += `</tbody></table></div>`;
  html += `<div class="dl-row"><button class="dl-btn" onclick="downloadCSV('${stmtKey}')">↓ Download CSV</button></div>`;
  panel.innerHTML = html;
}

function renderRatiosPanel(panelId, ratios, periods, labels) {
  const panel   = $(panelId);
  const colSpan = periods.length + 1;
  const groups  = [
    { label: 'Profitability', keys: ['Gross Margin','Operating Margin','Net Profit Margin','Return on Equity','Return on Assets'] },
    { label: 'Liquidity',     keys: ['Current Ratio','Quick Ratio'] },
    { label: 'Leverage',      keys: ['Debt-to-Equity','Interest Coverage'] },
    { label: 'Efficiency',    keys: ['Asset Turnover'] },
  ];

  let html = buildMeta('ratios');
  html += `<div class="table-scroll"><table class="fin-table">`;
  html += buildHeader(labels, 'Ratio');
  html += `<tbody>`;

  for (const group of groups) {
    html += `<tr class="section-row"><td colspan="${colSpan}">${group.label}</td></tr>`;
    for (const key of group.keys) {
      const row = ratios.find(r => r.label === key);
      if (!row) continue;
      html += `<tr><td>${row.label} <span class="unit-badge">(${row.unit})</span></td>`;
      periods.forEach((p, i) => {
        const v   = row.values?.[p];
        const cls = [i === 0 ? 'col-latest' : '', valClass(v)].filter(Boolean).join(' ');
        html += `<td class="${cls}">${fmt(v, row.unit)}</td>`;
      });
      html += `</tr>`;
    }
  }

  html += `</tbody></table></div>`;
  html += `<div class="dl-row"><button class="dl-btn" onclick="downloadCSV('ratios')">↓ Download CSV</button></div>`;
  panel.innerHTML = html;
}

// ── Table builder helpers ──────────────────────────────────────────────────

const STMT_TITLES = {
  income_statement: 'Income Statement',
  balance_sheet:    'Balance Sheet',
  cash_flow:        'Cash Flow Statement',
  ratios:           'Financial Ratios',
};

function buildMeta(stmtKey) {
  return `<div class="table-meta">
    <span>${STMT_TITLES[stmtKey] || ''} · USD in Millions · Parentheses = negative</span>
    <span class="proxy-badge">✓ Flask / SEC EDGAR</span>
  </div>`;
}

function buildHeader(labels, firstCol = 'Line Item') {
  let html = `<thead><tr><th>${firstCol}</th>`;
  labels.forEach((l, i) => {
    html += `<th class="${i === 0 ? 'col-latest' : ''}">${l}</th>`;
  });
  return html + `</tr></thead>`;
}

// ── Formatting ─────────────────────────────────────────────────────────────

function fmt(v, unit) {
  if (v == null) return '—';
  if (unit === '%') return `${v.toFixed(1)}%`;
  if (unit === 'x') return `${v.toFixed(2)}x`;
  const abs = Math.abs(v);
  const s   = abs >= 1000 ? Math.round(abs).toLocaleString() : abs.toFixed(1);
  return v < 0 ? `(${s})` : s;
}

function valClass(v) {
  if (v == null) return '';
  return v > 0 ? 'val-pos' : v < 0 ? 'val-neg' : '';
}

// ── CSV download ───────────────────────────────────────────────────────────

function downloadCSV(stmtKey) {
  if (!state.data) return;
  const { ticker, periods, labels, statements, ratios } = state.data;

  let rows, title;
  if (stmtKey === 'ratios') {
    rows  = ratios.map(r => ({ label: `${r.label} (${r.unit})`, values: r.values }));
    title = 'Financial Ratios';
  } else {
    rows  = (statements[stmtKey] || []);
    title = STMT_TITLES[stmtKey] || stmtKey;
  }

  let csv = `# ${ticker} | ${title} | ${state.filing} | Units: USD Millions\n`;
  csv    += ['Line Item', ...labels].join(',') + '\n';

  for (const row of rows) {
    const vals = periods.map(p => row.values?.[p] ?? '');
    csv += ['"' + row.label.replace(/"/g, '""') + '"', ...vals].join(',') + '\n';
  }

  const a = Object.assign(document.createElement('a'), {
    href:     URL.createObjectURL(new Blob([csv], { type: 'text/csv' })),
    download: `${ticker}_${stmtKey}_${state.filing}.csv`,
  });
  a.click();
  URL.revokeObjectURL(a.href);
}