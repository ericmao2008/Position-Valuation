import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

// ---------- utils ----------
const numOrDefault = (v, d) => {
  if (v === undefined || v === null) return d;
  const s = String(v).trim();
  if (s === "") return d;
  const n = Number(s);
  return Number.isFinite(n) ? n : d;
};
const todayStr = (tz = "Asia/Shanghai") => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: tz }));
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
};
const TZ = process.env.TZ || "Asia/Shanghai";
const ERP_TARGET = numOrDefault(process.env.ERP_TARGET, 0.0527);
const DELTA      = numOrDefault(process.env.DELTA,      0.005);
const RF_OVERRIDE = numOrDefault(process.env.RF_OVERRIDE, 0.0178);
const PE_OVERRIDE = (() => {
  const s = (process.env.PE_OVERRIDE ?? "").toString().trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) && n > 0 && n < 1000 ? n : null;
})();
const USE_PLAYWRIGHT = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";

// ---------- Google Sheets ----------
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if (!SPREADSHEET_ID) { console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL, null,
  (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version: "v4", auth });

// ---------- PE: Danjuan ----------
async function getPE_real() {
  // JSON 1
  try {
    const r = await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=SH000300", {
      headers: { "User-Agent":"Mozilla/5.0", "Referer":"https://danjuanfunds.com" }, timeout: 8000
    });
    if (r.ok) {
      const j = await r.json();
      const v = Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm);
      if (Number.isFinite(v) && v>0 && v<1000) return v;
    }
  } catch {}
  // JSON 2（REST 变体）
  try {
    const r = await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail/SH000300", {
      headers: { "User-Agent":"Mozilla/5.0", "Referer":"https://danjuanfunds.com" }, timeout: 8000
    });
    if (r.ok) {
      const j = await r.json();
      const v = Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm);
      if (Number.isFinite(v) && v>0 && v<1000) return v;
    }
  } catch {}
  // HTML 内嵌 JSON
  try {
    const r = await fetch("https://danjuanfunds.com/index-detail/SH000300", { headers:{ "User-Agent":"Mozilla/5.0" }, timeout:8000 });
    if (r.ok) {
      const html = await r.text();
      const m = html.match(/"pe_ttm"\s*:\s*"?([0-9]+(?:\.[0-9]+)?)"?/i);
      if (m) { const v = Number(m[1]); if (Number.isFinite(v) && v>0 && v<1000) return v; }
      const text = html.replace(/<[^>]+>/g," ");
      const regs = [/PE[^0-9]{0,6}([0-9]+(?:\.[0-9]+)?)/i, /市盈率（?TTM）?[^0-9]{0,10}([0-9]+(?:\.[0-9]+)?)/];
      for (const re of regs) {
        const mm = text.match(re);
        if (mm) { const v = Number(mm[1]); if (Number.isFinite(v) && v>0 && v<1000) return v; }
      }
    }
  } catch {}
  // Playwright 兜底（可选）
  if (USE_PLAYWRIGHT) {
    try {
      const { chromium } = await import("playwright");
      const browser = await chromium.launch({ headless: true });
      const page = await browser.newPage();
      page.setDefaultNavigationTimeout(10000); page.setDefaultTimeout(8000);
      await page.goto("https://danjuanfunds.com/index-detail/SH000300", { waitUntil:"domcontentloaded" });
      let v = null;
      try {
        const resp = await page.waitForResponse(
          r => r.url().includes("/djapi/index_evaluation/detail") && r.status()===200,
          { timeout:9000 }
        );
        const data = await resp.json();
        v = Number(data?.data?.pe_ttm ?? data?.data?.pe ?? data?.data?.valuation?.pe_ttm);
      } catch {}
      if (!Number.isFinite(v)) {
        const text = await page.locator("body").innerText();
        const m = text.match(/(PE|市盈率)[^0-9]{0,6}([0-9]+(?:\.[0-9]+)?)/i);
        if (m) v = Number(m[2]);
      }
      await browser.close();
      if (Number.isFinite(v) && v>0 && v<1000) return v;
    } catch {}
  }
  return null;
}

async function getPE_fallback_from_lastSheet() {
  // 读取最近一个“YYYY-MM-DD”标签页的 P/E（B3）
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const sheetsList = (meta.data.sheets || [])
    .map(s => s.properties?.title)
    .filter(t => /^\d{4}-\d{2}-\d{2}$/.test(t))
    .sort(); // 字典序 ~ 时间序

  const last = sheetsList[sheetsList.length - 1];
  if (!last) return null;
  try {
    const r = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `'${last}'!B3:B3` // B3 = P/E 数值
    });
    const v = Number(r.data.values?.[0]?.[0]);
    return Number.isFinite(v) && v>0 && v<1000 ? v : null;
  } catch { return null; }
}

// ---------- RF: Youzhiyouxing ----------
async function getRF_real() {
  try {
    const r = await fetch("https://youzhiyouxing.cn/data", { headers:{ "User-Agent":"Mozilla/5.0" }, timeout:6000 });
    if (r.ok) {
      const html = await r.text();
      const m = html.match(/10年期国债到期收益率[^%]{0,120}?(\d+(?:\.\d+)?)\s*%/);
      if (m) {
        const v = Number(m[1])/100;
        if (Number.isFinite(v) && v>0 && v<1) return v;
      }
    }
  } catch {}
  return null;
}

// ---------- 写入 Sheet：新增“数据”列（真实/兜底） ----------
async function upsertDailySheet(rows, tz=TZ) {
  const title = todayStr(tz);
  // 获取/创建当日标签
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  let sheet = meta.data.sheets?.find(s => s.properties?.title === title);
  if (!sheet) {
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests:[{ addSheet:{ properties:{ title } } }] }
    });
    sheet = { properties: add.data?.replies?.[0]?.addSheet?.properties };
  }
  const range = `'${title}'!A1:E${rows.length}`;
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range, valueInputOption:"USER_ENTERED",
    requestBody: { values: rows }
  });

  // 简单格式：百分比在 B/E 列无关，这里省略复杂格式（前面已满足你的可读性）
}

// ---------- 邮件（可选） ----------
async function maybeSendEmail(payload) {
  const { SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_TO, MAIL_FROM_NAME } = process.env;
  if (!SMTP_HOST || !SMTP_USER || !SMTP_PASS || !MAIL_TO) return;
  const transporter = nodemailer.createTransport({
    host: SMTP_HOST, port: Number(SMTP_PORT||465), secure: Number(SMTP_PORT||465)===465,
    auth: { user: SMTP_USER, pass: SMTP_PASS }
  });
  const pct = v => v==null? "-" : (v*100).toFixed(2)+"%";
  const html = `
    <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.6">
      <p>已更新：<b>${payload.date}</b></p>
      <ul>
        <li>P/E（TTM）：<b>${payload.pe}</b>（${payload.peTag}）</li>
        <li>10Y：<b>${pct(payload.rf)}</b>（${payload.rfTag}）</li>
        <li>隐含ERP：<b>${pct(payload.impliedERP)}</b></li>
        <li>判定：<b>${payload.status}</b></li>
      </ul>
      <p><a target="_blank" href="https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/edit#gid=0">在线打开总表</a></p>
    </div>`;
  await transporter.sendMail({
    from: `"${MAIL_FROM_NAME || "Valuation Bot"}" <${SMTP_USER}>`,
    to: MAIL_TO, subject:`[估值] 沪深300（${payload.date}）— ${payload.status}`, html
  });
}

// ---------- Main ----------
(async () => {
  const date = todayStr(TZ);

  // PE：真实→回读历史→PE_OVERRIDE
  let pe = await getPE_real();
  let peTag = "真实";
  if (pe == null) { pe = await getPE_fallback_from_lastSheet(); peTag = pe==null? peTag : "兜底"; }
  if (pe == null && PE_OVERRIDE != null) { pe = PE_OVERRIDE; peTag = "兜底"; }

  // RF：真实→RF_OVERRIDE
  let rf = await getRF_real();
  let rfTag = "真实";
  if (rf == null) { rf = RF_OVERRIDE; rfTag = "兜底"; }

  // 计算（缺 P/E 则不算）
  const ep = pe ? (1/pe) : null;
  const impliedERP = (ep!=null && rf!=null) ? (ep - rf) : null;
  const peLimit = (rf!=null) ? Number((1/(rf + ERP_TARGET)).toFixed(2)) : null;

  let status = "需手动更新";
  if (impliedERP != null) {
    if (impliedERP >= ERP_TARGET + DELTA) status = "买点（低估）";
    else if (impliedERP <= ERP_TARGET - DELTA) status = "卖点（高估）";
    else status = "持有（合理）";
  }

  // 5列表头：字段 | 数值 | 数据 | 说明 | 数据源
  const link = {
    csi:  '=HYPERLINK("https://www.csindex.com.cn/zh-CN/indices/index-detail/000300","中证指数有限公司")',
    dan:  '=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")',
    yzyx: '=HYPERLINK("https://youzhiyouxing.cn/data","Youzhiyouxing")',
    dam:  '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'
  };

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数","沪深300","真实","本工具演示以沪深300为例，可扩展", link.csi],
    ["P/E（TTM）", pe ?? "", peTag, "蛋卷基金 index-detail（JSON→HTML）", link.dan],
    ["E/P = 1 / P/E", ep ?? "", pe ? "真实" : "兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rfTag, "有知有行（文本；抓不到用兜底）", link.yzyx],
    ["隐含ERP = E/P − r_f", impliedERP ?? "", (impliedERP!=null) ? "真实" : "兜底", "市场给予的风险补偿（小数，显示为百分比）", "—"],
    ["目标 ERP*", ERP_TARGET, "真实", "建议参考达摩达兰", link.dam],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换", "—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", (peLimit!=null) ? "真实" : "兜底", "直观对照", "—"],
    ["判定", status, (impliedERP!=null) ? "真实" : "兜底", "买点/持有/卖点/需手动", "—"],
    ["信号图标", status.startsWith("买点")?"🟢":status.startsWith("卖点")?"🔴":status==="持有（合理）"?"🟡":"⚪", "真实", "🟢=买点，🟡=持有，🔴=卖点，⚪=需手动", "—"]
  ];

  await upsertDailySheet(rows, TZ);

  console.log("[DEBUG]", { date, pe, peTag, rf, rfTag, ep, impliedERP, peLimit, status });

  await maybeSendEmail({ date, pe, peTag, rf, rfTag, impliedERP, status });
})().catch(e => { console.error(e); process.exit(1); });
