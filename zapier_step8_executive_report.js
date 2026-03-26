// ============================================================================
// EXECUTIVE SALES REPORT — STEP 8 (Code by Zapier)
//
// Zapier Workflow: "Geoff: Sales Reports"
// Schedule: Every Wednesday at 6:00 PM EST
// Recipient: geoff@idegy.com (for David Brown)
// CC: lawrence@idegy.com, antonietta@idegy.com
//
// This code step replaces the Google Drive CSV read path with direct
// Supabase queries. Data is populated by GitHub Actions at 5 PM EST
// (1 hour before this Zap fires).
//
// Inputs (Configure tab):
//   - supabaseKey: Supabase anon key (static secret)
//
// Outputs:
//   - emailSubject: Executive report subject line
//   - htmlEmail: Full HTML email body
//
// CHANGELOG:
//   2026-03-26: Rebuilt to read from Supabase instead of Google Drive.
//               Tables: commonsku_sr_weekly, commonsku_sr_monthly, commonsku_sr_ytd
//               on project oascilobkhxpmrayftar.supabase.co.
// ============================================================================

// ============================================================================
// CONSTANTS
// ============================================================================
const SUPABASE_URL = 'https://oascilobkhxpmrayftar.supabase.co';
const SUPABASE_KEY = inputData.supabaseKey;

const EXCLUDED_REPS = ['House Account', 'Idegy Accounting', 'House', 'Idegy Accounting2'];
const HOUSE_REP_NAMES = ['Antonietta MacKenzie', 'David Brown'];

// Active sales rep roster
const ACTIVE_REPS = [
  'Amy Richardson', 'Dane Price', 'Farren Clemenzi', 'Jordyn Cody',
  'Kara Herzog', 'Katie Davis', 'Kelly Vesselis', 'Leigh Wolnik',
  'Luke Kiley', 'Matt Deighton', 'Tara Parke'
];

// Date calculation (Eastern time)
const getEasternDate = () => {
  const now = new Date();
  const eastern = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const year = eastern.getFullYear();
  const month = String(eastern.getMonth() + 1).padStart(2, '0');
  const day = String(eastern.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

const TODAY = getEasternDate();
const dateObj = new Date(TODAY + 'T12:00:00');
const reportDate = dateObj.toLocaleDateString('en-US', {
  weekday: 'long',
  year: 'numeric',
  month: 'long',
  day: 'numeric'
});

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================
function parseNumber(value) {
  if (!value) return 0;
  const cleaned = String(value).replace(/[\$,]/g, '');
  return parseFloat(cleaned) || 0;
}

function formatCurrency(value) {
  if (value === null || value === undefined || isNaN(value)) return '\$0';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
  }).format(Math.round(value));
}

function getGPColor(gpPercent) {
  const gp = parseFloat(gpPercent);
  if (isNaN(gp)) return '#64748B';
  if (gp >= 35) return '#059669';
  if (gp >= 33) return '#D97706';
  return '#DC2626';
}

function isExcludedRep(repName) {
  if (!repName) return true;
  return EXCLUDED_REPS.some(excluded =>
    repName.toLowerCase().includes(excluded.toLowerCase())
  ) || HOUSE_REP_NAMES.includes(repName);
}

// ============================================================================
// SUPABASE DATA FETCHER
// ============================================================================
async function getSupabaseData(tableName) {
  const url = `${SUPABASE_URL}/rest/v1/${tableName}?export_date=eq.${TODAY}&select=*`;
  const resp = await fetch(url, {
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`
    }
  });

  if (!resp.ok) {
    const errorText = await resp.text();
    throw new Error(`Supabase ${tableName}: ${resp.status} ${resp.statusText} - ${errorText}`);
  }

  const rows = await resp.json();
  if (!rows.length) return [];
  return rows;
}

// ============================================================================
// PARSE SR DATA (works with Supabase row objects directly)
// ============================================================================
function parseSRData(rows) {
  const repSummary = {};
  let companyTotal = 0;
  let companyMarginWeighted = 0;
  let companyRevenueWithMargin = 0;
  let companyOrderCount = 0;

  for (const row of rows) {
    // Skip non-sales-order rows
    const orderType = (row.order_type || '').toUpperCase();
    if (orderType !== 'SALES ORDER') continue;

    const salesRep = `${row.sales_rep_first_name || ''} ${row.sales_rep_last_name || ''}`.trim();
    const clientRep = `${row.client_rep_first_name || ''} ${row.client_rep_last_name || ''}`.trim();
    const repName = clientRep || salesRep || 'Unknown';
    const subtotal = parseNumber(row.subtotal);
    const gpPercent = parseNumber(row.booked_margin);

    // Company totals (before rep exclusion)
    companyTotal += subtotal;
    companyOrderCount += 1;
    if (gpPercent > 0 && subtotal > 0) {
      companyMarginWeighted += gpPercent * subtotal;
      companyRevenueWithMargin += subtotal;
    }

    // Skip excluded reps from individual breakdown
    if (isExcludedRep(repName)) continue;

    if (!repSummary[repName]) {
      repSummary[repName] = {
        name: repName,
        salesTotal: 0,
        orderCount: 0,
        marginWeighted: 0,
        revenueWithMargin: 0
      };
    }

    repSummary[repName].salesTotal += subtotal;
    repSummary[repName].orderCount += 1;
    if (gpPercent > 0 && subtotal > 0) {
      repSummary[repName].marginWeighted += gpPercent * subtotal;
      repSummary[repName].revenueWithMargin += subtotal;
    }
  }

  // Calculate GP% for each rep
  const repList = Object.values(repSummary).map(rep => ({
    ...rep,
    gpPercent: rep.revenueWithMargin > 0
      ? (rep.marginWeighted / rep.revenueWithMargin).toFixed(1)
      : '0.0'
  }));

  repList.sort((a, b) => b.salesTotal - a.salesTotal);

  const companyGP = companyRevenueWithMargin > 0
    ? (companyMarginWeighted / companyRevenueWithMargin).toFixed(1)
    : '0.0';

  return {
    reps: repList,
    companyTotal,
    companyOrderCount,
    companyGP
  };
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

// Fetch all three datasets from Supabase
const weeklyRows = await getSupabaseData('commonsku_sr_weekly');
const monthlyRows = await getSupabaseData('commonsku_sr_monthly');
const ytdRows = await getSupabaseData('commonsku_sr_ytd');

// Parse each dataset
const weeklyData = parseSRData(weeklyRows);
const monthlyData = parseSRData(monthlyRows);
const ytdData = parseSRData(ytdRows);

// ============================================================================
// BUILD HTML EMAIL
// ============================================================================

const cardStyle = `
  background: #FFFFFF;
  border-radius: 12px;
  padding: 24px;
  margin-bottom: 16px;
  border: 1px solid #E2E8F0;
`;

const heroCardStyle = `
  background: linear-gradient(135deg, #1E293B 0%, #334155 100%);
  color: #FFFFFF;
  border-radius: 12px;
  padding: 28px;
  text-align: center;
  margin-bottom: 8px;
`;

const sectionHeaderStyle = `
  font-size: 16px;
  font-weight: 700;
  color: #1E293B;
  margin: 24px 0 12px 0;
  padding-bottom: 8px;
  border-bottom: 2px solid #3B82F6;
`;

function buildRepTable(data, periodLabel) {
  if (!data.reps.length) {
    return `<p style="color:#94A3B8;font-style:italic;">No sales order data for ${periodLabel}.</p>`;
  }

  let tableHTML = `
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
      <thead>
        <tr style="background:#F1F5F9;">
          <th style="padding:10px 12px;text-align:left;font-weight:600;color:#475569;border-bottom:2px solid #CBD5E1;">Sales Rep</th>
          <th style="padding:10px 12px;text-align:right;font-weight:600;color:#475569;border-bottom:2px solid #CBD5E1;">Orders</th>
          <th style="padding:10px 12px;text-align:right;font-weight:600;color:#475569;border-bottom:2px solid #CBD5E1;">Sales Total</th>
          <th style="padding:10px 12px;text-align:right;font-weight:600;color:#475569;border-bottom:2px solid #CBD5E1;">GP%</th>
        </tr>
      </thead>
      <tbody>
  `;

  data.reps.forEach((rep, index) => {
    const rowBg = index % 2 === 0 ? '#FFFFFF' : '#F8FAFC';
    const gpColor = getGPColor(rep.gpPercent);
    tableHTML += `
      <tr style="background:${rowBg};">
        <td style="padding:10px 12px;border-bottom:1px solid #E2E8F0;font-weight:500;color:#1E293B;">${rep.name}</td>
        <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #E2E8F0;color:#64748B;">${rep.orderCount}</td>
        <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #E2E8F0;font-weight:600;color:#1E293B;">${formatCurrency(rep.salesTotal)}</td>
        <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #E2E8F0;font-weight:700;color:${gpColor};">${rep.gpPercent}%</td>
      </tr>
    `;
  });

  // Company totals row
  const companyGPColor = getGPColor(data.companyGP);
  tableHTML += `
      <tr style="background:#F1F5F9;font-weight:700;">
        <td style="padding:12px;border-top:2px solid #3B82F6;color:#1E293B;">COMPANY TOTAL</td>
        <td style="padding:12px;text-align:right;border-top:2px solid #3B82F6;color:#1E293B;">${data.companyOrderCount}</td>
        <td style="padding:12px;text-align:right;border-top:2px solid #3B82F6;color:#1E293B;">${formatCurrency(data.companyTotal)}</td>
        <td style="padding:12px;text-align:right;border-top:2px solid #3B82F6;color:${companyGPColor};">${data.companyGP}%</td>
      </tr>
    </tbody>
    </table>
  `;

  return tableHTML;
}

const htmlEmail = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Executive Sales Report</title>
</head>
<body style="margin:0;padding:0;background:#F1F5F9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="max-width:680px;margin:0 auto;padding:24px;">

    <!-- Header -->
    <div style="text-align:center;margin-bottom:24px;">
      <h1 style="font-size:22px;font-weight:800;color:#1E293B;margin:0;">Executive Sales Report</h1>
      <p style="font-size:14px;color:#64748B;margin:4px 0 0 0;">${reportDate}</p>
    </div>

    <!-- Hero Cards: Weekly | Monthly | YTD -->
    <div style="display:flex;gap:8px;margin-bottom:24px;">
      <div style="${heroCardStyle}flex:1;">
        <div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;opacity:0.8;margin-bottom:4px;">This Week</div>
        <div style="font-size:28px;font-weight:800;">${formatCurrency(weeklyData.companyTotal)}</div>
        <div style="font-size:12px;opacity:0.7;margin-top:4px;">GP: ${weeklyData.companyGP}%</div>
      </div>
      <div style="${heroCardStyle}flex:1;">
        <div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;opacity:0.8;margin-bottom:4px;">This Month</div>
        <div style="font-size:28px;font-weight:800;">${formatCurrency(monthlyData.companyTotal)}</div>
        <div style="font-size:12px;opacity:0.7;margin-top:4px;">GP: ${monthlyData.companyGP}%</div>
      </div>
      <div style="${heroCardStyle}flex:1;">
        <div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;opacity:0.8;margin-bottom:4px;">Year to Date</div>
        <div style="font-size:28px;font-weight:800;">${formatCurrency(ytdData.companyTotal)}</div>
        <div style="font-size:12px;opacity:0.7;margin-top:4px;">GP: ${ytdData.companyGP}%</div>
      </div>
    </div>

    <!-- Weekly Performance -->
    <div style="${cardStyle}">
      <div style="${sectionHeaderStyle}">Weekly Performance</div>
      ${buildRepTable(weeklyData, 'this week')}
    </div>

    <!-- Monthly Performance -->
    <div style="${cardStyle}">
      <div style="${sectionHeaderStyle}">Monthly Performance</div>
      ${buildRepTable(monthlyData, 'this month')}
    </div>

    <!-- YTD Performance -->
    <div style="${cardStyle}">
      <div style="${sectionHeaderStyle}">Year-to-Date Performance</div>
      ${buildRepTable(ytdData, 'year-to-date')}
    </div>

    <!-- GP% Legend -->
    <div style="${cardStyle}text-align:center;padding:16px;">
      <span style="font-size:12px;color:#64748B;">GP% Indicators: </span>
      <span style="font-size:12px;color:#059669;font-weight:600;">&#9679; 35%+ Good</span>
      <span style="font-size:12px;color:#64748B;"> &nbsp;|&nbsp; </span>
      <span style="font-size:12px;color:#D97706;font-weight:600;">&#9679; 33-34% Fair</span>
      <span style="font-size:12px;color:#64748B;"> &nbsp;|&nbsp; </span>
      <span style="font-size:12px;color:#DC2626;font-weight:600;">&#9679; Below 33% Concerning</span>
    </div>

    <!-- Footer -->
    <div style="text-align:center;padding:16px 0;color:#94A3B8;font-size:11px;">
      <p style="margin:0;">idegy Inc. &mdash; Executive Sales Report</p>
      <p style="margin:4px 0 0 0;">Generated ${reportDate} at 6:00 PM EST</p>
      <p style="margin:4px 0 0 0;font-style:italic;">Data source: CommonSKU &rarr; GitHub Actions &rarr; Supabase</p>
    </div>

  </div>
</body>
</html>`;

const emailSubject = `Executive Sales Report — ${reportDate} | YTD: ${formatCurrency(ytdData.companyTotal)} (GP: ${ytdData.companyGP}%) | Month: ${formatCurrency(monthlyData.companyTotal)}`;

output = {
  emailSubject,
  htmlEmail,
  weeklyTotal: formatCurrency(weeklyData.companyTotal),
  monthlyTotal: formatCurrency(monthlyData.companyTotal),
  ytdTotal: formatCurrency(ytdData.companyTotal),
  ytdGP: ytdData.companyGP + '%',
  dataStatus: `Weekly: ${weeklyRows.length} rows, Monthly: ${monthlyRows.length} rows, YTD: ${ytdRows.length} rows`
};
