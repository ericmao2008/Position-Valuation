// === Danjuan PE → Google Sheet (Existing Workbook, daily new tab + email) ===
// 功能：
// 1) 蛋卷 P/E 无登录抓取（JSON → HTML → 可选 Playwright兜底，仅在 USE_PLAYWRIGHT='1' 时启用）
// 2) 有知有行 10Y（先纯文本；可选 Playwright兜底；也支持 RF_OVERRIDE）
// 3) 写入“总表”：每日新建 YYYY-MM-DD 标签页，设置数值格式与超链接
// 4) 邮件通知（SMTP 可选）：未配置则自动跳过

import fetch from 'node-fetch';
import nodemailer from 'nodemailer';
import { google } from 'googleapis';
import { format } from 'date-fns';

// --------------------------------- 常量 / 环境 ---------------------------------
const INDEX_CODE = 'SH000300';
const DJ_JSON = `https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=${INDEX_CODE}`;
const DJ_HTML = `https://danjuanfunds.com/index-detail/${INDEX_CODE}`;
const CSI_HOME = 'https://www.csindex.com.cn/zh-CN/indices/index-detail/000300';

const YZYX_DATA = 'https://youzhiyouxing.cn/data';
const DAMODARAN_CRP = 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html';

// 数字解析安全函数
const numOrDefault = (v, d) => {
  if (v === undefined || v === null) return d;
  const s = String(v).trim();
  if (s === '') return d;
  const n = Number(s);
  return Number.isFinite(n) ? n : d;
};

// 估值参数（允许不配置，自动回落到默认值）
const ERP_TARGET = numOrDefault(process.env.ERP_TARGET, 0.0527); // 5.27%
const DELTA      = numOrDefault(process.env.DELTA,      0.005);  // 0.50%

// 性能/稳定性开关
const tz              = process.env.TZ || 'Asia/Shanghai';
const USE_PLAYWRIGHT  = String(process.env.USE_PLAYWRIGHT ?? '0') === '1';
const RF_OVERRIDE     = (process.env.RF_OVERRIDE && String(process.env.RF_OVERRIDE).trim() !== '')
  ? Number(process.env.RF_OVERRIDE) : null; // 例如 0.0178（小数）

const todayStr = () => format(new Date(new Date().toLocaleString('en-US', { timeZone: tz })), 'yyyy-MM-dd');

// --------------------------------- 抓取 P/E（蛋卷） ---------------------------------
async function getPE_fromJSON() {
  try {
    const res = await fetch(DJ_JSON, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Referer': DJ_HTML, 'Accept': 'application/json' },
      timeout: 8000
    });
    if (!res.ok) return null;
    const j = await res.json();
    const pe = Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm);
    return pe > 0 && pe < 1000 ? pe : null;
  } catch { return null; }
}

async function getPE_fromHTML() {
  try {
    const res = await fetch(DJ_HTML, { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 8000 });
    if (!res.ok) return null;
    const html = await res.text();
    const regs = [
      /PE[^0-9]{0,4}([0-9]+(?:\.[0-9]+)?)/i,
      /市盈率（?TTM）?[^0-9]{0,10}([0-9]+(?:\.[0-9]+)?)/,
    ];
    for (const re of regs) {
      const m = html.match(re);
      if (m) { const pe = Number(m[1]); if (pe > 0 && pe < 1000) return pe; }
    }
  } catch {}
  return null;
}

// 仅在 USE_PLAYWRIGHT='1' 时才会调用（避免未安装浏览器时报错）
async function getPE_withPlaywrightFallback() {
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(10000);
  page.setDefaultTimeout(8000);
  await page.goto(DJ_HTML, { waitUntil: 'domcontentloaded' });

  let pe = null;
  try {
    const resp = await page.waitForResponse(
      r => r.url().includes('/djapi/index_evaluation/detail') && r.status() === 200,
      { timeout: 9000 }
    );
    const data = await resp.json();
    pe = Number(data?.data?.pe_ttm ?? data?.data?.pe ?? data?.data?.valuation?.pe_ttm);
  } catch {}

  if (!pe || !(pe > 0)) {
    try {
      const text = await page.locator('body').innerText();
      const m = text.match(/(PE|市盈率)[^0-9]{0,6}([0-9]+(?:\.[0-9]+)?)/i);
      if (m) pe = Number(m[2]);
    } catch {}
  }
  await browser.close();
  return pe && pe > 0 && pe < 1000 ? pe : null;
}

// --------------------------------- 抓取 10Y（有知有行） ---------------------------------
async function getChina10Y() {
  // A) 文本解析 —— 最快
  try {
    const res = await fetch(YZYX_DATA, { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 6000 });
    if (res.ok) {
      const html = await res.text();
      const m = html.match(/10年期国债到期收益率[^%]{0,120}?(\d+(?:\.\d+)?)\s*%/);
      if (m) { const v = Number(m[1]); if (v > 0.1 && v < 10) return v / 100; }
    }
  } catch {}

  // B) 渲染兜底（仅在 USE_PLAYWRIGHT='1' 时启用）
  if (USE_PLAYWRIGHT) {
    try {
      const { chromium } = await import('playwright');
      const browser = await chromium.launch({ headless: true });
      const page = await browser.newPage();
      page.setDefaultNavigationTimeout(10000);
      page.setDefaultTimeout(8000);

      await page.goto(YZYX_DATA, { waitUntil: 'domcontentloaded' });
      const loc = page.locator('text=10年期国债到期收益率').first();
      if (await loc.count() > 0) {
        const handle = await loc.elementHandle();
        const box = await handle.evaluateHandle(el => el.closest('section,div,li,article') || el.parentElement);
        const text = await box.evaluate(el => el.innerText || '');
        let rf = null;
        const m = text.match(/10年期国债到期收益率[^%]{0,120}?(\d+(?:\.\d+)?)\s*%/);
        if (m) rf = Number(m[1]);
        if (!rf) {
          const arr = [...text.matchAll(/(\d+(?:\.\d+)?)\s*%/g)].map(x => Number(x[1]));
          if (arr.length) rf = Math.max(...arr);
        }
        await browser.close();
        if (rf && rf > 0.1 && rf < 10) return rf / 100;
      } else {
        await browser.close();
      }
    } catch {}
  }

  // C) 覆盖值（最后兜底）
  if (Number.isFinite(RF_OVERRIDE)) return RF_OVERRIDE;

  return null;
}

// --------------------------------- 写入总表 + 邮件 ---------------------------------
async function writeToExistingWorkbookAndEmail({ pe, rf }) {
  const auth = new google.auth.JWT(
    process.env.GOOGLE_CLIENT_EMAIL,
    undefined,
    (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
    ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
  );
  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = process.env.SPREADSHEET_ID;
  if (!spreadsheetId) throw new Error('缺少 SPREADSHEET_ID（请设置为你手动创建的“总表”ID并共享给服务账号为编辑者）。');

  // 计算
  const ep = pe ? 1 / pe : null;
  const impliedERP = (ep != null && rf != null) ? (ep - rf) : null;
  const peLimit = (rf != null) ? (1 / (rf + ERP_TARGET)) : null;

  const status = (impliedERP == null)
    ? '需手动更新'
    : (impliedERP >= ERP_TARGET + DELTA ? '买点（低估）'
      : (impliedERP <= ERP_TARGET - DELTA ? '卖点（高估）' : '持有（合理）'));
  const icon = status.startsWith('买点') ? '🟢'
    : status.startsWith('卖点') ? '🔴'
    : (status === '需手动更新' ? '⚪' : '🟡');

  // 数据源链接（HYPERLINK）
  const linkCSI           = `=HYPERLINK("${CSI_HOME}","中证指数有限公司")`;
  const linkDanjuan       = `=HYPERLINK("${DJ_HTML}","Danjuan")`;
  const linkYouzhiyouxing = `=HYPERLINK("${YZYX_DATA}","Youzhiyouxing")`;
  const linkDamodaran     = `=HYPERLINK("${DAMODARAN_CRP}","Damodaran")`;
  const dash = '—';

  const values = [
    ['字段','数值','说明','数据源'],
    ['指数','沪深300','本工具演示以沪深300为例，可扩展', linkCSI],
    ['P/E（TTM）', pe, '蛋卷基金 index-detail（JSON/HTML/渲染）', linkDanjuan],
    ['E/P = 1 / P/E', ep, '盈收益率（小数，显示为百分比）', dash],
    ['无风险利率 r_f（10Y名义）', rf, '有知有行（文本优先；可选渲染兜底）', linkYouzhiyouxing],
    ['隐含ERP = E/P − r_f', impliedERP, '市场给予的风险补偿（小数，显示为百分比）', dash],
    ['目标 ERP*', ERP_TARGET, '建议参考达摩达兰', linkDamodaran],
    ['容忍带 δ', DELTA, '减少频繁切换', dash],
    ['对应P/E上限 = 1/(r_f + ERP*)', peLimit, '直观对照', dash],
    ['判定', status, '买点/持有/卖点/需手动', dash],
    ['信号图标', icon, '🟢=买点，🟡=持有，🔴=卖点，⚪=需手动', dash]
  ];

  // 新建当日标签页（存在则忽略）
  const sheetTitle = todayStr();
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests: [{ addSheet: { properties: { title: sheetTitle } } }] }
  }).catch(e => { if (!String(e).includes('already exists')) throw e; });

  // 写入内容
  const range = `'${sheetTitle}'!A1:D12`;
  await sheets.spreadsheets.values.update({
    spreadsheetId, range, valueInputOption: 'USER_ENTERED', requestBody: { values }
  });

  // 数值格式
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const tar = meta.data.sheets.find(s => s.properties.title === sheetTitle);
  if (tar && typeof tar.properties.sheetId === 'number') {
    const sheetId = tar.properties.sheetId;
    const cell = r => ({ sheetId, startRowIndex: r, endRowIndex: r + 1, startColumnIndex: 1, endColumnIndex: 2 });
    const reqs = [];
    // 百分比：E/P、r_f、隐含ERP、ERP*、δ（B2=1，B3=2…这里以 0 起始行号：第 4 行索引3）
    [3,4,5,6,7].forEach(r => reqs.push({
      repeatCell: { range: cell(r), cell: { userEnteredFormat: { numberFormat: { type: 'NUMBER', pattern: '0.00%' } } }, fields: 'userEnteredFormat.numberFormat' }
    }));
    // 两位小数：P/E、P/E上限
    [2,8].forEach(r => reqs.push({
      repeatCell: { range: cell(r), cell: { userEnteredFormat: { numberFormat: { type: 'NUMBER', pattern: '0.00' } } }, fields: 'userEnteredFormat.numberFormat' }
    }));
    await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests: reqs } });
  }

  // 调试输出
  console.log('[DEBUG decision]', 'E/P=', ep, 'r_f=', rf, 'impliedERP=', impliedERP, 'target=', ERP_TARGET, 'delta=', DELTA, '→', status);
  const link = `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit#gid=0`;
  console.log('Wrote to:', link, 'tab=', sheetTitle);

  // 邮件（SMTP 未配齐则自动跳过）
  await sendEmail({ link, sheetTitle, status, ep, rf, impliedERP, pe, peLimit });
}

async function sendEmail({ link, sheetTitle, status, ep, rf, impliedERP, pe, peLimit }) {
  const { SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS } = process.env;
  const to = process.env.MAIL_TO;
  if (!SMTP_HOST || !SMTP_USER || !SMTP_PASS || !to) {
    console.warn('[MAIL] SMTP/Mail env not set, skip email.');
    return;
  }
  const transporter = nodemailer.createTransport({
    host: SMTP_HOST, port: Number(SMTP_PORT || 465), secure: Number(SMTP_PORT || 465) === 465,
    auth: { user: SMTP_USER, pass: SMTP_PASS }
  });
  const fromName = process.env.MAIL_FROM_NAME || 'Valuation Bot';
  const subject = `[估值] 沪深300（${sheetTitle}）— ${status}`;
  const pct = v => v == null ? '-' : (v * 100).toFixed(2) + '%';
  const html = `
    <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.6">
      <p>已更新当日估值标签：<b>${sheetTitle}</b></p>
      <ul>
        <li>P/E（TTM）：<b>${pe?.toFixed?.(2) ?? '-'}</b></li>
        <li>E/P：<b>${pct(ep)}</b></li>
        <li>10Y名义：<b>${pct(rf)}</b></li>
        <li>隐含ERP：<b>${pct(impliedERP)}</b></li>
        <li>P/E 上限：<b>${peLimit ? peLimit.toFixed(2) : '-'}</b></li>
        <li>判定：<b>${status}</b></li>
      </ul>
      <p><a href="${link}" target="_blank">👉 打开“持仓估值总表”（在线查看）</a></p>
    </div>`;
  await transporter.sendMail({ from: `"${fromName}" <${SMTP_USER}>`, to, subject, html });
  console.log('[MAIL] sent to', to);
}

// --------------------------------- 主流程 ---------------------------------
(async () => {
  // 先用 JSON/HTML；只有在 USE_PLAYWRIGHT='1' 且依然失败时才渲染兜底
  let pe = await getPE_fromJSON();
  if (!pe) pe = await getPE_fromHTML();
  if (!pe && USE_PLAYWRIGHT) pe = await getPE_withPlaywrightFallback();

  const rf = await getChina10Y(); // 文本优先；可选渲染兜底；支持 RF_OVERRIDE

  if (!pe) console.warn('警告：未从蛋卷拿到 P/E。');
  if (rf == null) console.warn('警告：未能获取 10Y（有知有行）。如需强制覆盖，设置 RF_OVERRIDE=小数（如 0.0178）。');

  await writeToExistingWorkbookAndEmail({ pe, rf });
})().catch(e => { console.error(e); process.exit(1); });
