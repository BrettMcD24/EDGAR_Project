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
    // Divider fires BEFORE the keyed row (= first row of new section)
    'Cost of Revenue':             'Cost & Gross Margin',
    'R&D Expense':                 'Operating Expenses',
    'Sales & Marketing':           'Operating Expenses',
    'SG&A Expense':                'Operating Expenses',
    'Operating Income (Loss)':     'Operating Result',
    'Interest Expense':            'Non-Operating Items',
    'Other Income (Expense), Net': 'Non-Operating Items',
    'Pre-Tax Income (Loss)':       'Tax & Net Income',
    'EPS – Basic':                 'Per Share Data',
  },
  balance_sheet: {
    // Each entry: "row label" → "section header to show above this row"
    // The renderer tracks the last header emitted and only fires when it changes,
    // so multiple rows sharing the same section name only get ONE divider.
    'PP&E, Net':                        'Non-Current Assets',
    'Real Estate Investment, Net':      'Non-Current Assets',
    'Goodwill':                         'Non-Current Assets',
    'Intangible Assets, Net':           'Non-Current Assets',
    'Operating Lease ROU Assets':       'Non-Current Assets',
    'Other Non-Current Assets':         'Non-Current Assets',
    'Total Assets':                     'Total Assets',
    'Accounts Payable':                 'Current Liabilities',
    'Accrued Liabilities':              'Current Liabilities',
    'Current Portion – LT Debt':        'Current Liabilities',
    'Total Current Liabilities':        'Current Liabilities',
    'Long-Term Debt':                   'Non-Current Liabilities',
    'Operating Lease Liability':        'Non-Current Liabilities',
    'Deferred Tax Liabilities':         'Non-Current Liabilities',
    'Other Non-Current Liabilities':    'Non-Current Liabilities',
    'Total Liabilities':                'Total Liabilities',
    'Additional Paid-In Capital':       'Equity',
    'Retained Earnings (Deficit)':      'Equity',
    'Total Stockholders\'s Equity':     'Equity',
    'Total Liabilities & Equity':       'Total',
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

  let lastSection = null;   // track last header emitted — prevents duplicate dividers

  for (const row of rows) {
    const sectionHeader = breaks[row.label];
    // Only emit divider when the section label CHANGES (not for every row in the section)
    if (sectionHeader && sectionHeader !== lastSection) {
      html += `<tr class="section-row"><td colspan="${colSpan}">${sectionHeader}</td></tr>`;
      lastSection = sectionHeader;
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

// ═══════════════════════════════════════════════════════════════════════════
// COMPARISON TOOL
// ═══════════════════════════════════════════════════════════════════════════

// ── Comparison state ───────────────────────────────────────────────────────
const cmpState = {
  filing:  '10-K',
  periods: 3,
  data:    [null, null],   // [company1, company2]
};

// ── Comparison controls ────────────────────────────────────────────────────
function setCmpFiling(el) {
  document.querySelectorAll('[data-cmp-filing]').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  cmpState.filing = el.dataset.cmpFiling;
}

function setCmpPeriods(el) {
  document.querySelectorAll('[data-cmp-n]').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  cmpState.periods = parseInt(el.dataset.cmpN);
}

function switchCmpTab(el) {
  document.querySelectorAll('#cmpTabs .tab-btn').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  document.querySelectorAll('.cmp-panel').forEach(p => p.classList.remove('active'));
  $(`cmp${el.dataset.cmpTab.charAt(0).toUpperCase() + el.dataset.cmpTab.slice(1)}Panel`).classList.add('active');
}

function cmpQuick(t1, t2) {
  $('cmpTicker1').value = t1;
  $('cmpTicker2').value = t2;
  loadComparison();
}

// Enter key on either input triggers comparison
['cmpTicker1', 'cmpTicker2'].forEach(id => {
  const el = $(id);
  if (el) el.addEventListener('keydown', e => { if (e.key === 'Enter') loadComparison(); });
});

// ── Status helpers ─────────────────────────────────────────────────────────
function showCmpStatus(msg, type = 'loading') {
  const el = $('cmpStatus');
  el.className = type;
  el.innerHTML = type === 'loading'
    ? `<div class="spinner"></div><span>${msg}</span>`
    : `<span>${msg}</span>`;
}
function hideCmpStatus() {
  $('cmpStatus').className = '';
  $('cmpStatus').style.display = '';
}

// ── Main: fetch both companies in parallel ─────────────────────────────────
async function loadComparison() {
  const t1 = $('cmpTicker1').value.trim().toUpperCase();
  const t2 = $('cmpTicker2').value.trim().toUpperCase();

  if (!t1 || !t2) {
    showCmpStatus('Enter tickers for both companies.', 'error');
    return;
  }
  if (t1 === t2) {
    showCmpStatus('Enter two different tickers to compare.', 'error');
    return;
  }

  $('cmpBtn').disabled = true;
  $('cmpBanners').style.display = 'none';
  $('cmpTabs').style.display    = 'none';
  $('cmpArea').style.display    = 'none';
  $('cmpEmpty').style.display   = 'none';

  showCmpStatus(`Loading ${t1} and ${t2} simultaneously…`);

  try {
    // Fetch both companies in parallel
    const [r1, r2] = await Promise.all([
      fetch(`/api/financials?ticker=${t1}&filing=${cmpState.filing}&periods=${cmpState.periods}`),
      fetch(`/api/financials?ticker=${t2}&filing=${cmpState.filing}&periods=${cmpState.periods}`),
    ]);

    const [d1, d2] = await Promise.all([r1.json(), r2.json()]);

    if (!r1.ok) throw new Error(`${t1}: ${d1.error || 'Error'}`);
    if (!r2.ok) throw new Error(`${t2}: ${d2.error || 'Error'}`);

    cmpState.data = [d1, d2];
    hideCmpStatus();
    renderComparison(d1, d2);

  } catch (e) {
    showCmpStatus(e.message || 'Unexpected error.', 'error');
    $('cmpEmpty').style.display = 'block';
  } finally {
    $('cmpBtn').disabled = false;
  }
}

// ── Render banners ─────────────────────────────────────────────────────────
function renderCmpBanner(id, data, colorClass) {
  const el = $(id);
  const latestLabel = data.labels?.[0] || '';
  el.className = `cmp-banner ${colorClass}`;
  el.innerHTML = `
    <div class="ticker-badge">${data.ticker}</div>
    <div>
      <div class="company-name">${data.company}</div>
      <div class="company-sub">CIK ${data.cik} · ${data.filing} · ${data.labels?.join(' · ') || ''}</div>
    </div>`;
}

// ── Get value from data by label ───────────────────────────────────────────
function cmpGetVal(data, label, periodDate) {
  for (const stmt of Object.values(data.statements || {})) {
    for (const row of stmt) {
      if (row.label === label) return row.values?.[periodDate] ?? null;
    }
  }
  return null;
}

// ── Build a single comparison table (one statement, two companies) ─────────
function buildCmpTable(d1, d2, stmtKey) {
  const rows1 = d1.statements?.[stmtKey] || [];
  const rows2 = d2.statements?.[stmtKey] || [];
  const breaks = SECTION_BREAKS[stmtKey] || {};

  // Union of all labels preserving order from company 1, then appending any
  // labels unique to company 2
  const labels1 = rows1.map(r => r.label);
  const labels2 = rows2.map(r => r.label);
  const allLabels = [...labels1, ...labels2.filter(l => !labels1.includes(l))];

  // Build lookup maps: label → values dict
  const map1 = Object.fromEntries(rows1.map(r => [r.label, r.values]));
  const map2 = Object.fromEntries(rows2.map(r => [r.label, r.values]));

  // Most recent period date for each company
  const p1 = d1.periods?.[0];
  const p2 = d2.periods?.[0];
  const l1 = d1.labels?.[0] || d1.ticker;
  const l2 = d2.labels?.[0] || d2.ticker;

  // Build two mini tables side by side
  function miniTable(data, map, ticker, periods, labels, colClass) {
    let html = `<div class="cmp-col-label ${colClass}">${ticker} — ${STMT_TITLES[stmtKey] || stmtKey}</div>`;
    html += `<div class="table-scroll"><table class="fin-table">`;
    html += `<thead><tr><th>Line Item</th>`;
    labels.forEach((l, i) => {
      html += `<th class="${i === 0 ? 'col-latest' : ''}">${l}</th>`;
    });
    html += `</tr></thead><tbody>`;

    let lastSection = null;
    for (const rowLabel of allLabels) {
      const sectionHeader = breaks[rowLabel];
      if (sectionHeader && sectionHeader !== lastSection) {
        html += `<tr class="section-row"><td colspan="${periods.length + 1}">${sectionHeader}</td></tr>`;
        lastSection = sectionHeader;
      }
      const values = map[rowLabel] || {};
      html += `<tr><td>${rowLabel}</td>`;
      periods.forEach((p, i) => {
        const v   = values[p] ?? null;
        const cls = [i === 0 ? 'col-latest' : '', valClass(v)].filter(Boolean).join(' ');
        html += `<td class="${cls}">${fmt(v)}</td>`;
      });
      html += `</tr>`;
    }
    html += `</tbody></table></div>`;
    return html;
  }

  return `<div class="cmp-tables-wrap">
    <div class="cmp-table-col">
      ${miniTable(d1, map1, d1.ticker, d1.periods, d1.labels, '')}
    </div>
    <div class="cmp-table-col">
      ${miniTable(d2, map2, d2.ticker, d2.periods, d2.labels, 'col2')}
    </div>
  </div>`;
}

// ── Build comparison ratios panel ──────────────────────────────────────────
function buildCmpRatios(d1, d2) {
  const ratios1 = d1.ratios || [];
  const ratios2 = d2.ratios || [];

  const groups = [
    { label: 'Profitability', keys: ['Gross Margin','Operating Margin','Net Profit Margin','Return on Equity','Return on Assets'] },
    { label: 'Liquidity',     keys: ['Current Ratio','Quick Ratio'] },
    { label: 'Leverage',      keys: ['Debt-to-Equity','Interest Coverage'] },
    { label: 'Efficiency',    keys: ['Asset Turnover'] },
  ];

  function ratioMiniTable(ratios, data, colClass) {
    const p0 = data.periods?.[0];
    const labels = data.labels || [];
    let html = `<div class="cmp-col-label ${colClass}">${data.ticker} — Financial Ratios</div>`;
    html += `<div class="table-scroll"><table class="fin-table">`;
    html += `<thead><tr><th>Ratio</th>`;
    labels.forEach((l, i) => html += `<th class="${i === 0 ? 'col-latest' : ''}">${l}</th>`);
    html += `</tr></thead><tbody>`;

    for (const group of groups) {
      html += `<tr class="section-row"><td colspan="${labels.length + 1}">${group.label}</td></tr>`;
      for (const key of group.keys) {
        const row = ratios.find(r => r.label === key);
        if (!row) continue;
        html += `<tr><td>${row.label} <span class="unit-badge">(${row.unit})</span></td>`;
        data.periods.forEach((p, i) => {
          const v   = row.values?.[p] ?? null;
          const cls = [i === 0 ? 'col-latest' : '', valClass(v)].filter(Boolean).join(' ');
          html += `<td class="${cls}">${fmt(v, row.unit)}</td>`;
        });
        html += `</tr>`;
      }
    }
    html += `</tbody></table></div>`;
    return html;
  }

  return `<div class="cmp-tables-wrap">
    <div class="cmp-table-col">${ratioMiniTable(ratios1, d1, '')}</div>
    <div class="cmp-table-col">${ratioMiniTable(ratios2, d2, 'col2')}</div>
  </div>`;
}

// ── Main render ────────────────────────────────────────────────────────────
function renderComparison(d1, d2) {
  // Banners
  renderCmpBanner('cmpBanner1', d1, '');
  renderCmpBanner('cmpBanner2', d2, 'col2');
  $('cmpBanners').style.display = 'grid';

  // Tabs — reset to income
  document.querySelectorAll('#cmpTabs .tab-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.cmpTab === 'income'));
  document.querySelectorAll('.cmp-panel').forEach(p =>
    p.classList.toggle('active', p.id === 'cmpIncomePanel'));

  $('cmpIncomePanel').innerHTML   = buildCmpTable(d1, d2, 'income_statement');
  $('cmpBalancePanel').innerHTML  = buildCmpTable(d1, d2, 'balance_sheet');
  $('cmpCashflowPanel').innerHTML = buildCmpTable(d1, d2, 'cash_flow');
  $('cmpRatiosPanel').innerHTML   = buildCmpRatios(d1, d2);

  $('cmpTabs').style.display  = 'flex';
  $('cmpArea').style.display  = 'block';
}