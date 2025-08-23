// === Danjuan PE → Google Sheet (Existing Workbook, daily new tab + email) ===
import fetch from 'node-fetch';
import nodemailer from 'nodemailer';
import { chromium } from 'playwright';
import { google } from 'googleapis';
import { format } from 'date-fns';

// --------- 常量 / 环境 ---------
const INDEX_CODE = 'SH000300';
const DJ_JSON = `https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=${INDEX_CODE}`;
const DJ_HTML = `https://danjuanfunds.com/index-detail/${INDEX_CODE}`;
const CSI_HOME = 'https://www.csindex.com.cn/zh-CN/indices/index-detail/000300';

const YZYX_DATA = 'https://youzhiyouxing.cn/data';
const DAMODARAN_CRP = 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html';

const ERP_TARGET = Number(process.env.ERP_TARGET ?? 0.0527);
const DELTA      = Number(process.env.DELTA ?? 0.005);
const tz         = process.env.TZ || 'Asia/Shanghai';

function todayStr() {
  const now = new Date();
  const local = new Date(now.toLocaleString('en-US', { timeZone: tz }));
  return format(local, 'yyyy-MM-dd'); // YYYY-MM-DD
}

// --------- 蛋卷 P/E（无登录三重抓取）---------
async function getPE_fromJSON() {
  try {
    const res = await fetch(DJ_JSON, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Referer': DJ_HTML, 'Accept': 'application/json' }
    });
    if (!res.ok) return null;
    const j = await res.json();
    const pe = Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm);
    return pe > 0 && pe < 1000 ? pe : null;
  } catch { return null; }
}
async function getPE_fromHTML() {
  try {
    const res = await fetch(DJ_HTML, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    if (!res.ok) return null;
    const html = await res.text();
    const regs = [
      /PE[^0-9]{0,4}([0-9]+(?:\.[0-9]+)?)/i,
      /市盈率（?TTM）?[^0-9]{0,10}([0-9]+(?:\.[0-9]+)?)/,
    ];
    for (const re of regs) {
      const m = html.match(re);
      if (m) {
        const pe = Number(m[1]);
        if (pe > 0 && pe < 1000) return pe;
      }
    }
  } catch {}
  return null;
}
async function getPE_withPlaywrightFallback() {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.goto(DJ_HTML, { waitUntil: 'domcontentloaded' });
  let pe = null;
  try {
    const resp = await page.waitForResponse(
      r => r.url().includes('/djapi/index_evaluation/detail') && r.status() === 200,
      { timeout: 12000 }
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

// ---------- 10Y（名义，小数）只用“有知有行” ----------
async function getChina10Y() {
  // A) Playwright 精准定位“10年期国债到期收益率”卡片
  try {
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    await page.goto(YZYX_DATA, { waitUntil: 'domcontentloaded' });

    const loc = page.locator('text=10年期国债到期收益率').first();
    if (await loc.count() > 0) {
      const handle = await loc.elementHandle();
      const box = await handle.evaluateHandle(el => el.closest('section,div,li,article') || el.parentElement);
      const text = await box.evaluate(el => el.innerText || '');
      let rfPct = null;
      const m = text.match(/10年期国债到期收益率[^%]{0,120}?(\d+(?:\.\d+)?)\s*%/);
      if (m) rfPct = Number(m[1]);
      if (!rfPct) {
        const mm = [...text.matchAll(/(\d+(?:\.\d+)?)\s*%/g)].map(x => Number(x[1]));
        if (mm.length) rfPct = Math.max(...mm);
      }
      await browser.close();
      if (rfPct && rfPct > 0.1 && rfPct < 10) return rfPct / 100;
    } else {
      await browser.close();
    }
  } catch {}

  // B) 纯文本解析同一页面（作为轻量兜底，但仍然只来自有知有行）
  try {
    const res = await fetch(YZYX_DATA, { headers: { 'User-Agent': 'Mozilla/5.0' }});
    if (res.ok) {
      const html = await res.text();
      const m = html.match(/10年期国债到期收益率[^%]{0,120}?(\d+(?:\.\d+)?)\s*%/);
      if (m) { const v = Number(m[1]); if (v > 0.1 && v < 10) return v / 100; }
    }
  } catch {}

  // 若两种方式都失败，返回 null（不再使用其他网站）
  return null;
}

// --------- 写入“总表”：每日新 tab + 数字格式 + 数据源链接 + 邮件 ---------
async function writeToExistingWorkbookAndEmail({ pe, rf }) {
  const auth = new google.auth.JWT(
    process.env.GOOGLE_CLIENT_EMAIL,
    undefined,
    (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
    ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
  );
  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = process.env.SPREADSHEET_ID;
  if (!spreadsheetId) throw new Error('缺少 SPREADSHEET_ID（请设置为你手动创建的“总表”ID并共享给服务账号为编辑者）。');

  // 计算（内部小数；展示再格式化为百分比）
  const ep = pe ? 1 / pe : null;                        // 0.0716 → 7.16%
  const impliedERP = (ep != null && rf != null) ? (ep - rf) : null;
  const peLimit = (rf != null) ? (1 / (rf + ERP_TARGET)) : null;

  const status = (impliedERP == null)
    ? '需手动更新'
    : (impliedERP >= ERP_TARGET + DELTA ? '买点（低估）'
      : (impliedERP <= ERP_TARGET - DELTA ? '卖点（高估）' : '持有（合理）'));
  const icon = status.startsWith('买点') ? '🟢'
              : status.startsWith('卖点') ? '🔴'
              : (status === '需手动更新' ? '⚪' : '🟡');

  // —— “数据源链接”列（HYPERLINK 公式） ——
  const linkCSI       = `=HYPERLINK("${CSI_HOME}","中证指数有限公司")`;
  const linkDanjuan   = `=HYPERLINK("${DJ_HTML}","Danjuan")`;
  const linkYouzhiyou = `=HYPERLINK("${YZYX_DATA}","Youzhiyouxing")`;
  const linkDamodaran = `=HYPERLINK("${DAMODARAN_CRP}","Damodaran")`;
  const dash          = '—';

  const values = [
    ['字段','数值','说明','数据源'],
    ['指数','沪深300','本工具演示以沪深300为例，可扩展', linkCSI],
    ['P/E（TTM）', pe, '蛋卷基金 index-detail（JSON/HTML/渲染）', linkDanjuan],
    ['E/P = 1 / P/E', ep, '盈收益率（小数，显示为百分比）', dash],
    ['无风险利率 r_f（10Y名义）', rf, '有知有行（只用该站）', linkYouzhiyou],
    ['隐含ERP = E/P − r_f', impliedERP, '市场给予的风险补偿（小数，显示为百分比）', dash],
    ['目标 ERP*', ERP_TARGET, '建议参考达摩达兰（国家风险溢价/成熟市场ERP）', linkDamodaran],
    ['容忍带 δ', DELTA, '减少频繁切换', dash],
    ['对应P/E上限 = 1/(r_f + ERP*)', peLimit, '直观对照', dash],
    ['判定', status, '买点/持有/卖点/需手动', dash],
    ['信号图标', icon, '🟢=买点，🟡=持有，🔴=卖点，⚪=需手动', dash]
  ];

  // 新建当日 tab（存在则忽略）
  const sheetTitle = todayStr();
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests: [{ addSheet: { properties: { title: sheetTitle } } }] }
  }).catch(e => { if (!String(e).includes('already exists')) throw e; });

  // 写入数据
  const range = `'${sheetTitle}'!A1:D12`;
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values }
  });

  // 数字格式：百分比 / 两位小数
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const tar = meta.data.sheets.find(s => s.properties.title === sheetTitle);
  if (tar && typeof tar.properties.sheetId === 'number') {
    const sheetId = tar.properties.sheetId;
    const reqs = [];
    const cell = (r) => ({
      sheetId, startRowIndex: r, endRowIndex: r + 1, startColumnIndex: 1, endColumnIndex: 2
    });
    const ROW_P_E=2, ROW_EP=3, ROW_RF=4, ROW_IMP_ERP=5, ROW_TARGET_ERP=6, ROW_DELTA=7, ROW_PE_LIMIT=8;
    [ROW_EP, ROW_RF, ROW_IMP_ERP, ROW_TARGET_ERP, ROW_DELTA].forEach(r => {
      reqs.push({ repeatCell: { range: cell(r),
        cell: { userEnteredFormat: { numberFormat: { type:'NUMBER', pattern:'0.00%' } } },
        fields: 'userEnteredFormat.numberFormat' } });
    });
    [ROW_P_E, ROW_PE_LIMIT].forEach(r => {
      reqs.push({ repeatCell: { range: cell(r),
        cell: { userEnteredFormat: { numberFormat: { type:'NUMBER', pattern:'0.00' } } },
        fields: 'userEnteredFormat.numberFormat' } });
    });
    await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests: reqs } });
  }

  // 判定调试
  console.log('[DEBUG decision]', 'E/P=', ep, 'r_f=', rf, 'impliedERP=', impliedERP,
    'target=', ERP_TARGET, 'delta=', DELTA, '→', status);
  const link = `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit#gid=0`;
  console.log('Wrote to:', link, 'tab=', sheetTitle);

  // 邮件通知（如未配置 SMTP 则自动跳过）
  await sendEmail({ link, sheetTitle, status, ep, rf, impliedERP, pe, peLimit });
}

// --------- 发邮件（SMTP）---------
async function sendEmail({ link, sheetTitle, status, ep, rf, impliedERP, pe, peLimit }) {
  const host = process.env.SMTP_HOST;
  const port = Number(process.env.SMTP_PORT || 465);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  const to   = process.env.MAIL_TO;
  if (!host || !user || !pass || !to) {
    console.warn('[MAIL] SMTP/Mail env not set, skip email.');
    return;
  }
  const transporter = nodemailer.createTransport({
    host, port, secure: port === 465, auth: { user, pass }
  });
  const fromName = process.env.MAIL_FROM_NAME || 'Valuation Bot';
  const subject = `[估值] 沪深300（${sheetTitle}）— ${status}`;
  const html = `
    <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.6">
      <p>已更新当日估值标签：<b>${sheetTitle}</b></p>
      <ul>
        <li>P/E（TTM）：<b>${pe?.toFixed?.(2) ?? '-'}</b></li>
        <li>E/P：<b>${(ep!=null? (ep*100).toFixed(2)+'%':'-')}</b></li>
        <li>10Y名义：<b>${(rf!=null? (rf*100).toFixed(2)+'%':'-')}</b></li>
        <li>隐含ERP：<b>${(impliedERP!=null? (impliedERP*100).toFixed(2)+'%':'-')}</b></li>
        <li>P/E 上限：<b>${peLimit? peLimit.toFixed(2) : '-'}</b></li>
        <li>判定：<b>${status}</b></li>
      </ul>
      <p><a href="${link}" target="_blank">👉 打开“持仓估值总表”（在线查看）</a></p>
    </div>`;
  await transporter.sendMail({ from: `"${fromName}" <${user}>`, to, subject, html });
  console.log('[MAIL] sent to', to);
}

// --------- 主流程 ---------
(async () => {
  let pe = await getPE_fromJSON();
  if (!pe) pe = await getPE_fromHTML();
  if (!pe) pe = await getPE_withPlaywrightFallback();

  const rf = await getChina10Y();
  if (!pe) console.warn('警告：未从蛋卷拿到 P/E（JSON/HTML/Playwright 均失败）。');
  if (rf == null) console.warn('警告：未能获取 10Y 国债收益率（仅用有知有行）。');

  await writeToExistingWorkbookAndEmail({ pe, rf });
})().catch(e => { console.error(e); process.exit(1); });
