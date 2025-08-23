import { chromium } from 'playwright';
import fetch from 'node-fetch';
import { google } from 'googleapis';
import { format } from 'date-fns';

const STORAGE = process.env.STORAGE_STATE || 'storage_state.json';
const INDEX_URL = 'https://danjuanfunds.com/index-detail/SH000300';
const DJAPI_MATCH = '/djapi/index_evaluation/detail?index_code=SH000300';
const ERP_TARGET = Number(process.env.ERP_TARGET ?? 0.0527);   // 5.27%
const DELTA = Number(process.env.DELTA ?? 0.005);              // 0.50%

const tz = process.env.TZ || 'Asia/Shanghai';

function todayStr() {
  // format in Asia/Shanghai
  const now = new Date();
  // crude tz offset handling by toLocaleString
  const local = new Date(now.toLocaleString('en-US', { timeZone: tz }));
  return format(local, 'yyyy-MM-dd');
}

async function getChina10Y() {
  const headers = { 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.google.com' };
  // Eastmoney
  try {
    const res = await fetch('https://data.eastmoney.com/cjsj/zmgzsyl.html', { headers });
    const html = await res.text();
    const m = html.match(/中国[\s\S]{0,80}?10年[\s\S]{0,40}?(\d+\.\d+)\s*%/);
    if (m) return Number(m[1]) / 100;
  } catch {}
  // Investing (cn)
  try {
    const res = await fetch('https://cn.investing.com/rates-bonds/china-10-year-bond-yield', { headers });
    const html = await res.text();
    const m = html.match(/(\d+\.\d+)\s*%/);
    if (m) return Number(m[1]) / 100;
  } catch {}
  return null;
}

async function fetchPEFromDanjuanWithCookies() {
  const browser = await chromium.launch({ headless: true });
  const context = await chromium.newContext({ storageState: STORAGE });
  const page = await context.newPage();

  // 捕捉 JSON 接口
  const respPromise = page.waitForResponse(r => r.url().includes(DJAPI_MATCH) && r.status() === 200, { timeout: 20000 }).catch(() => null);
  await page.goto(INDEX_URL, { waitUntil: 'domcontentloaded' });

  let pe = null;
  try {
    const resp = await respPromise;
    if (resp) {
      const data = await resp.json();
      pe = Number(data?.data?.pe_ttm ?? data?.data?.pe ?? data?.data?.valuation?.pe_ttm);
    }
  } catch {}

  // DOM 兜底：尝试在页面文本中匹配 “市盈率（TTM）/PE TTM” 数字
  if (!pe || !(pe > 0)) {
    try {
      await page.waitForTimeout(4000);
      const bodyText = await page.locator('body').innerText();
      const m = bodyText.match(/(市盈率（?TTM）?|PE\s*TTM)[^0-9]{0,10}([0-9]+(?:\.[0-9]+)?)/i);
      if (m) pe = Number(m[2]);
    } catch {}
  }

  await context.close();
  await browser.close();
  return pe && pe > 0 && pe < 1000 ? pe : null;
}

async function createSheetAndWrite({ pe, rf }) {
  const auth = new google.auth.JWT(
    process.env.GOOGLE_CLIENT_EMAIL,
    undefined,
    (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
    ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
  );
  const sheets = google.sheets({ version: 'v4', auth });
  const drive = google.drive({ version: 'v3', auth });

  const title = `持仓估值${todayStr()}`;
  const { data: created } = await sheets.spreadsheets.create({
    requestBody: { properties: { title } }
  });
  const spreadsheetId = created.spreadsheetId;

  if (process.env.GOOGLE_FOLDER_ID) {
    await drive.files.update({
      fileId: spreadsheetId,
      addParents: process.env.GOOGLE_FOLDER_ID,
      removeParents: 'root',
      fields: 'id, parents'
    }).catch(()=>{});
  }

  const ep = pe ? 1/pe : null;
  const impliedERP = (ep != null && rf != null) ? (ep - rf) : null;
  const peLimit = (rf != null) ? (1/(rf + ERP_TARGET)) : null;
  const status = (impliedERP == null) ? '需手动更新'
    : (impliedERP >= ERP_TARGET + DELTA ? '买点（低估）'
      : (impliedERP <= ERP_TARGET - DELTA ? '卖点（高估）' : '持有（合理）'));
  const icon = status.startsWith('买点') ? '🟢' : status.startsWith('卖点') ? '🔴' : (status==='需手动更新' ? '⚪' : '🟡');

  const values = [
    ['字段','数值','说明','数据源'],
    ['指数','沪深300','本工具演示以沪深300为例，可扩展','中证指数有限公司'],
    ['P/E（TTM）', pe, '优先：蛋卷基金 index-detail','Danjuan index-detail'],
    ['E/P = 1 / P/E', ep, '盈收益率（小数）','—'],
    ['无风险利率 r_f（10Y名义）', rf, '中国10Y国债收益率（名义，小数）','Eastmoney/Investing'],
    ['隐含ERP = E/P − r_f', impliedERP, '市场给予的风险补偿（小数）','—'],
    ['目标 ERP*', ERP_TARGET, '可调整，建议参考达摩达兰','—'],
    ['容忍带 δ', DELTA, '减少频繁切换','—'],
    ['对应P/E上限 = 1/(r_f + ERP*)', peLimit, '直观对照','—'],
    ['判定', status, '买点/持有/卖点/需手动','—'],
    ['信号图标', icon, '🟢=买点，🟡=持有，🔴=卖点，⚪=需手动','—']
  ];

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: 'A1:D12',
    valueInputOption: 'USER_ENTERED',
    requestBody: { values }
  });

  // 格式设置
  const requests = [
    { repeatCell: { range: { sheetId: 0, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: 4 },
                    cell: { userEnteredFormat: { backgroundColor: { red: 0.95, green: 0.95, blue: 0.95 }, textFormat: { bold: true } } },
                    fields: 'userEnteredFormat(backgroundColor,textFormat)' } },
    # set number formats row by row
  ]

  // Build number format requests
  const numFmt = [
    None,        // header
    None,        // 指数
    '0.00',      // P/E
    '0.00%',     // E/P
    '0.00%',     // r_f
    '0.00%',     // 隐含ERP
    '0.00%',     // 目标ERP*
    '0.00%',     // δ
    '0.00',      // P/E上限
    None,        // 判定
    None         // 图标
  ];

  function a1(row,col){ return { sheetId: 0, startRowIndex: row-1, endRowIndex: row, startColumnIndex: col-1, endColumnIndex: col }; }
  const startRow = 2;
  for (let i=0;i<numFmt.length;i++){
    const f = numFmt[i];
    if (!f) continue;
    requests.push({
      repeatCell: { range: a1(startRow+i,2), cell: { userEnteredNumberFormat: { type: 'NUMBER', pattern: f } }, fields: 'userEnteredNumberFormat' }
    });
  }

  // 判定底色
  const statusRow = startRow + 8;
  const iconRow   = startRow + 9;
  const bg = icon==='🟢' ? {red:0.78,green:0.94,blue:0.81}
           : icon==='🟡' ? {red:1,green:0.95,blue:0.80}
           : icon==='🔴' ? {red:0.97,green:0.80,blue:0.68}
           : {red:0.91,green:0.91,blue:0.91};
  const fg = icon==='🟢' ? {red:0,green:0.38,blue:0}
           : icon==='🟡' ? {red:0.5,green:0.38,blue:0}
           : icon==='🔴' ? {red:0.61,green:0,blue:0}
           : {red:0.33,green:0.33,blue:0.33};
  requests.push({
    repeatCell: { range: a1(statusRow,2),
                  cell: { userEnteredFormat: { backgroundColor: bg, textFormat: { bold:true, foregroundColor: fg } } },
                  fields: 'userEnteredFormat(backgroundColor,textFormat)' }
  });
  // 放大图标
  requests.push({
    repeatCell: { range: a1(iconRow,2),
                  cell: { userEnteredFormat: { textFormat: { fontSize: 18 } } },
                  fields: 'userEnteredFormat(textFormat)' }
  });

  await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests } });

  const link = `https://docs.google.com/spreadsheets/d/${spreadsheetId}`;
  console.log('Created spreadsheet:', link);
  return link;
}

(async () => {
  const pe = await fetchPEFromDanjuanWithCookies();
  const rf = await getChina10Y();
  if (!pe) console.warn('警告：未能获取 P/E（请先运行 `npm run login` 在浏览器里登录一次，或登录后更新 storage_state.json）。');
  if (rf == null) console.warn('警告：未能获取 10Y 国债收益率，将在表格中显示“需手动更新”。');
  await createSheetAndWrite({ pe, rf });
})().catch(e => {
  console.error(e);
  process.exit(1);
});
