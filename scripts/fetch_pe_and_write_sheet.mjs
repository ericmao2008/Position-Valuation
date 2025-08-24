// === Global Index Valuation + HS300 Daily ===
// - HS300 详表（保持你的样式/百分比/表头配色/“数据=真实/兜底”）
// - Investing.com 各国10Y（抓不到→ RF_* 兜底）
// - Damodaran Country ERP* 自动解析
// - 全市场指数列表：S&P500 / Nasdaq-100 / Nikkei225 / ASX200 / NIFTY50 / VN-Index
//   * S&P500：multpl（真实）
//   * Nasdaq-100：Nasdaq indeks页（Playwright 兜底；否则 PE_OVERRIDE_NDX）
//   * Nikkei225：Nikkei 官方 PER（真实）
//   * ASX200：S&P DJI 页面（Playwright 兜底；否则 PE_OVERRIDE_ASX200）
//   * NIFTY50：NSE India API（真实）
//   * VN-Index：暂用 PE_OVERRIDE_VN（请提供稳定官方页后接入）
// - 当日 tab 覆盖写入（不跳过）；判定=🟢/🟡/🔴 合并显示

import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

// ---------- helpers ----------
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
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

// ---------- env / params ----------
const TZ = process.env.TZ || "Asia/Shanghai";

// HS300 params（保持）
const ERP_TARGET = numOrDefault(process.env.ERP_TARGET, 0.0527);
const DELTA      = numOrDefault(process.env.DELTA,      0.005);
const RF_OVERRIDE_CN = numOrDefault(process.env.RF_OVERRIDE, 0.0178);
const PE_OVERRIDE_CN = (() => { const s=(process.env.PE_OVERRIDE??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)&&n>0&&n<1000?n:null;})();

const USE_PLAYWRIGHT = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";

// 各国 rf 兜底（小数）
const RF_BACKUP = {
  USA:       numOrDefault(process.env.RF_US, 0.043),
  Japan:     numOrDefault(process.env.RF_JP, 0.010),
  Germany:   numOrDefault(process.env.RF_DE, 0.023),
  India:     numOrDefault(process.env.RF_IN, 0.071),
  Vietnam:   numOrDefault(process.env.RF_VN, 0.028),
  Australia: numOrDefault(process.env.RF_AU, 0.042),
  China:     RF_OVERRIDE_CN,
};

// 各指数 PE 兜底变量（小数）
const PE_OV = k => { const s=(process.env[k]??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)&&n>0&&n<1000?n:null; };

// 指数清单（国家用于 rf/ERP* 匹配）
const INDEX_LIST = [
  { key:"SPX",     name:"标普500",      country:"USA",       getPE: getPE_SPX_real,     peVar:"PE_OVERRIDE_SPX" },
  { key:"NDX",     name:"纳斯达克100",  country:"USA",       getPE: getPE_NDX_real,     peVar:"PE_OVERRIDE_NDX" },
  { key:"N225",    name:"日经225",      country:"Japan",     getPE: getPE_N225_real,    peVar:"PE_OVERRIDE_N225" },
  { key:"ASX200",  name:"澳洲ASX200",   country:"Australia", getPE: getPE_ASX200_real,  peVar:"PE_OVERRIDE_ASX200" },
  { key:"NIFTY50", name:"印度Nifty50",  country:"India",     getPE: getPE_NIFTY50_real, peVar:"PE_OVERRIDE_NIFTY50" },
  { key:"VNINDEX", name:"越南VN-Index", country:"Vietnam",   getPE: getPE_VN_real,      peVar:"PE_OVERRIDE_VN" },
];

// ---------- Google Sheets ----------
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if (!SPREADSHEET_ID) { console.error("缺少 SPREADSHEET_ID"); process.exit(1); }

const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL, null,
  (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version: "v4", auth });

// ---------- Investing.com 10Y ----------
const INVESTING_10Y_URLS = {
  USA:       ["https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield","https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield"],
  Japan:     ["https://www.investing.com/rates-bonds/japan-10-year-bond-yield","https://cn.investing.com/rates-bonds/japan-10-year-bond-yield"],
  Germany:   ["https://www.investing.com/rates-bonds/germany-10-year-bond-yield","https://cn.investing.com/rates-bonds/germany-10-year-bond-yield"],
  India:     ["https://www.investing.com/rates-bonds/india-10-year-bond-yield","https://cn.investing.com/rates-bonds/india-10-year-bond-yield"],
  Vietnam:   ["https://www.investing.com/rates-bonds/vietnam-10-year-bond-yield","https://cn.investing.com/rates-bonds/vietnam-10-year-bond-yield"],
  Australia: ["https://www.investing.com/rates-bonds/australia-10-year-bond-yield","https://cn.investing.com/rates-bonds/australia-10-year-bond-yield"],
  China:     ["https://www.investing.com/rates-bonds/china-10-year-bond-yield","https://cn.investing.com/rates-bonds/china-10-year-bond-yield"],
};

async function fetchInvesting10Y(urls) {
  for (const url of urls) {
    try {
      const res = await fetch(url, { headers:{ "User-Agent": UA, "Referer":"https://www.google.com" }, timeout:10000 });
      if (!res.ok) continue;
      const html = await res.text();
      const m = html.match(/(\d+(?:\.\d+)?)\s*%/);
      if (m) {
        const v = Number(m[1])/100;
        if (Number.isFinite(v) && v>0 && v<1) return v;
      }
    } catch {}
  }
  return null;
}
async function getRF_forCountry(countryKey) {
  const urls = INVESTING_10Y_URLS[countryKey] || [];
  const rfReal = await fetchInvesting10Y(urls);
  if (rfReal != null) return { rf: rfReal, tag: "真实" };
  return { rf: RF_BACKUP[countryKey], tag:"兜底" };
}

// ---------- Damodaran 国家ERP* ----------
async function fetchDamodaranERPMap() {
  const url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  const map = {};
  try {
    const res = await fetch(url, { headers:{ "User-Agent": UA }, timeout:12000 });
    if (!res.ok) return null;
    const html = await res.text();
    const rows = html.split(/<\/tr>/i);
    for (const row of rows) {
      const text = row.replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim();
      if (!text) continue;
      const mCountry = text.match(/^([A-Za-z .&()-]+)\s/);
      const mERP = text.match(/(\d+(?:\.\d+)?)\s*%/);
      if (mCountry && mERP) {
        const country = mCountry[1].trim();
        const erp = Number(mERP[1])/100;
        if (country && Number.isFinite(erp)) map[country] = erp;
      }
    }
    if (map["United States"]) map["USA"] = map["United States"];
    return map;
  } catch { return null; }
}

// ---------- HS300：Danjuan ----------
async function getPE_HS300_real() {
  try {
    const r = await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=SH000300", {
      headers:{ "User-Agent": UA, "Referer":"https://danjuanfunds.com" }, timeout:8000
    });
    if (r.ok) { const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm);
      if (Number.isFinite(v)&&v>0&&v<1000) return v; }
  } catch {}
  try {
    const r = await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail/SH000300", {
      headers:{ "User-Agent": UA, "Referer":"https://danjuanfunds.com" }, timeout:8000
    });
    if (r.ok) { const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm);
      if (Number.isFinite(v)&&v>0&&v<1000) return v; }
  } catch {}
  try {
    const r = await fetch("https://danjuanfunds.com/index-detail/SH000300", { headers:{ "User-Agent": UA }, timeout:8000 });
    if (r.ok) {
      const html = await r.text();
      const m = html.match(/"pe_ttm"\s*:\s*"?([0-9]+(?:\.[0-9]+)?)"?/i);
      if (m) { const v=Number(m[1]); if (Number.isFinite(v)&&v>0&&v<1000) return v; }
      const text = html.replace(/<[^>]+>/g," ");
      const regs = [/PE[^0-9]{0,6}([0-9]+(?:\.[0-9]+)?)/i,/市盈率（?TTM）?[^0-9]{0,10}([0-9]+(?:\.[0-9]+)?)/];
      for (const re of regs) { const mm=text.match(re); if (mm) { const v=Number(mm[1]); if (Number.isFinite(v)&&v>0&&v<1000) return v; } }
    }
  } catch {}
  if (USE_PLAYWRIGHT) {
    try {
      const { chromium } = await import("playwright");
      const browser = await chromium.launch({ headless:true });
      const page = await browser.newPage();
      page.setDefaultNavigationTimeout(10000); page.setDefaultTimeout(8000);
      await page.goto("https://danjuanfunds.com/index-detail/SH000300",{waitUntil:"domcontentloaded"});
      let v=null;
      try {
        const resp=await page.waitForResponse(r=>r.url().includes("/djapi/index_evaluation/detail")&&r.status()===200,{timeout:9000});
        const data=await resp.json();
        v=Number(data?.data?.pe_ttm ?? data?.data?.pe ?? data?.data?.valuation?.pe_ttm);
      } catch {}
      if (!Number.isFinite(v)) {
        const text=await page.locator("body").innerText();
        const m=text.match(/(PE|市盈率)[^0-9]{0,6}([0-9]+(?:\.[0-9]+)?)/i);
        if (m) v=Number(m[2]);
      }
      await browser.close();
      if (Number.isFinite(v)&&v>0&&v<1000) return v;
    } catch {}
  }
  return null;
}

async function getPE_fallback_lastSheet() {
  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const titles = (meta.data.sheets || []).map(s=>s.properties?.title).filter(t=>/^\d{4}-\d{2}-\d{2}$/.test(t)).sort();
    const last = titles[titles.length-1]; if (!last) return null;
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range:`'${last}'!B3:B3` });
    const v = Number(r.data.values?.[0]?.[0]); return Number.isFinite(v)&&v>0&&v<1000 ? v : null;
  } catch { return null; }
}

// ---------- 指数 PE 实时抓取函数 ----------
// S&P500: multpl
async function getPE_SPX_real() {
  try {
    const res = await fetch("https://www.multpl.com/s-p-500-pe-ratio", { headers:{ "User-Agent": UA }, timeout:10000 });
    if (res.ok) {
      const html = await res.text();
      // 页面有 "S&P 500 PE Ratio" + 当前值（可能在 <span class="current"> / 或图表描述）
      const m = html.match(/(\d+(?:\.\d+)?)(?=\s*(?:x|$))/i) || html.match(/current[^>]*>\s*([\d.]+)/i);
      if (m) { const v=Number(m[1]); if (Number.isFinite(v)&&v>0&&v<1000) return v; }
    }
  } catch {}
  return null;
}

// Nasdaq-100: Nasdaq index page（Playwright 兜底；否则 override）
async function getPE_NDX_real() {
  // 纯 HTML 很难稳定，建议仅在 USE_PLAYWRIGHT=1 时尝试
  if (!USE_PLAYWRIGHT) return null;
  try {
    const { chromium } = await import("playwright");
    const browser = await chromium.launch({ headless:true });
    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(10000); page.setDefaultTimeout(8000);
    await page.goto("https://www.nasdaq.com/market-activity/index/ndx", { waitUntil:"domcontentloaded" });
    // 页面上通常有 "P/E Ratio" 附近的数值
    const text = await page.locator("body").innerText();
    const m = text.match(/P\/E\s*Ratio[^0-9]*([\d.]+)/i);
    await browser.close();
    if (m) { const v=Number(m[1]); if (Number.isFinite(v)&&v>0&&v<1000) return v; }
  } catch {}
  return null;
}

// Nikkei 225：官方 PER
async function getPE_N225_real() {
  try {
    const res = await fetch("https://indexes.nikkei.co.jp/en/nkave", { headers:{ "User-Agent": UA }, timeout:10000 });
    if (res.ok) {
      const html = await res.text();
      const m = html.match(/PER[^0-9]*([\d.]+)/i);
      if (m) { const v=Number(m[1]); if (Number.isFinite(v)&&v>0&&v<1000) return v; }
    }
  } catch {}
  return null;
}

// ASX200：S&P DJI 页面（Playwright 兜底）
async function getPE_ASX200_real() {
  if (!USE_PLAYWRIGHT) return null;
  try {
    const { chromium } = await import("playwright");
    const browser = await chromium.launch({ headless:true });
    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(12000); page.setDefaultTimeout(10000);
    await page.goto("https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview", { waitUntil:"domcontentloaded" });
    const text = await page.locator("body").innerText();
    const m = text.match(/P\/E\s*(?:Ratio)?[^0-9]*([\d.]+)/i);
    await browser.close();
    if (m) { const v=Number(m[1]); if (Number.isFinite(v)&&v>0&&v<1000) return v; }
  } catch {}
  return null;
}

// NIFTY50：NSE India 官方 API
async function getPE_NIFTY50_real() {
  try {
    // NSE API 需要 UA、Referer；偶发 403 重试
    const res = await fetch("https://www.nseindia.com/api/allIndices", {
      headers:{ "User-Agent": UA, "Referer":"https://www.nseindia.com/" },
      timeout:12000
    });
    if (res.ok) {
      const j = await res.json();
      const row = (j?.data || []).find(r => (r?.index || "").toUpperCase().includes("NIFTY 50"));
      const v = Number(row?.pe);
      if (Number.isFinite(v) && v>0 && v<1000) return v;
    }
  } catch {}
  return null;
}

// VN-Index：暂时无稳定官方每日 PE，先用 override；你给出稳定页后我再接入
async function getPE_VN_real() { return null; }

// ---------- Sheet helpers ----------
async function ensureTodaySheetId() {
  const title = todayStr(TZ);
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  let found = meta.data.sheets?.find(s => s.properties?.title === title);
  if (!found) {
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests:[{ addSheet:{ properties:{ title } } }] }
    });
    found = { properties: add.data?.replies?.[0]?.addSheet?.properties };
  }
  return found.properties.sheetId;
}
async function writeBlock(rangeA1, rows) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID, range: rangeA1,
    valueInputOption:"USER_ENTERED",
    requestBody: { values: rows }
  });
}

// ---------- HS300 详表（与你现有完全一致的风格） ----------
async function writeHS300Block() {
  const sheetId = await ensureTodaySheetId();
  const date = todayStr(TZ);

  // P/E
  let pe = await getPE_HS300_real(); let peTag = "真实";
  if (pe == null) { const last = await getPE_fallback_lastSheet(); if (last != null) { pe=last; peTag="兜底"; } }
  if (pe == null && PE_OVERRIDE_CN != null) { pe = PE_OVERRIDE_CN; peTag="兜底"; }

  // r_f：Investing（统一口径）
  const { rf, tag: rfTag } = await getRF_forCountry("China");

  const ep = pe ? 1/pe : null;
  const impliedERP = (ep!=null && rf!=null) ? (ep - rf) : null;
  const peLimit = (rf!=null) ? Number((1/(rf + ERP_TARGET)).toFixed(2)) : null;
  let status = "需手动更新";
  if (impliedERP != null) {
    if (impliedERP >= ERP_TARGET + DELTA) status = "🟢 买点（低估）";
    else if (impliedERP <= ERP_TARGET - DELTA) status = "🔴 卖点（高估）";
    else status = "🟡 持有（合理）";
  }

  const link = {
    csi:  '=HYPERLINK("https://www.csindex.com.cn/zh-CN/indices/index-detail/000300","中证指数有限公司")',
    dan:  '=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")',
    invCN:'=HYPERLINK("https://cn.investing.com/rates-bonds/china-10-year-bond-yield","Investing China 10Y")',
    dam:  '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'
  };

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数","沪深300","真实","本工具演示以沪深300为例，可扩展", link.csi],
    ["P/E（TTM）", pe ?? "", peTag, "蛋卷基金 index-detail（JSON→HTML）", link.dan],
    ["E/P = 1 / P/E", ep ?? "", pe ? "真实" : "兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rfTag, "Investing.com 中国10年期国债收益率", link.invCN],
    ["隐含ERP = E/P − r_f", impliedERP ?? "", (impliedERP!=null) ? "真实" : "兜底", "市场给予的风险补偿（小数，显示为百分比）", "—"],
    ["目标 ERP*", ERP_TARGET, "真实", "建议参考达摩达兰", link.dam],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换", "—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", (peLimit!=null) ? "真实" : "兜底", "直观对照", "—"],
    ["判定", status, (impliedERP!=null) ? "真实" : "兜底", "买点/持有/卖点/需手动", "—"],
  ];

  await writeBlock(`'${date}'!A1:E${rows.length}`, rows);

  // 样式（表头灰底+加粗+居中、列宽、B列格式）
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody:{
      requests:[
        { repeatCell:{ range:{ sheetId, startRowIndex:0, endRowIndex:1, startColumnIndex:0, endColumnIndex:5 },
          cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
          fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } },
        { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:0, endIndex:1 }, properties:{ pixelSize:140 }, fields:"pixelSize" } },
        { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:1, endIndex:2 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
        { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:2, endIndex:3 }, properties:{ pixelSize:80  }, fields:"pixelSize" } },
        { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:3, endIndex:4 }, properties:{ pixelSize:420 }, fields:"pixelSize" } },
        { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:4, endIndex:5 }, properties:{ pixelSize:260 }, fields:"pixelSize" } },
        // 数值格式（B列）
        { repeatCell:{ range:{ sheetId, startRowIndex:2, endRowIndex:3, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
        { repeatCell:{ range:{ sheetId, startRowIndex:3, endRowIndex:8, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } }, fields:"userEnteredFormat.numberFormat" } },
        { repeatCell:{ range:{ sheetId, startRowIndex:8, endRowIndex:9, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
        // “数据”列（C）居中
        { repeatCell:{ range:{ sheetId, startRowIndex:1, startColumnIndex:2, endColumnIndex:3 }, cell:{ userEnteredFormat:{ horizontalAlignment:"CENTER" } }, fields:"userEnteredFormat.horizontalAlignment" } }
      ]
    }
  });

  return rows.length; // for summary start
}

// ---------- Summary: Global Index List (指数名称 | 当前PE | 估值水平) ----------
async function writeIndexSummary(startRow) {
  const date = todayStr(TZ);
  const sheetMeta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const sheetId = sheetMeta.data.sheets.find(s => s.properties?.title === date).properties.sheetId;

  const erpMap = (await fetchDamodaranERPMap()) || {};
  const table = [["指数名称","当前PE","估值水平"]];

  for (const idx of INDEX_LIST) {
    // 1) PE：优先真实抓取；失败 → override
    let peVal = await idx.getPE().catch(()=>null);
    if (!Number.isFinite(peVal) || peVal<=0) {
      const ov = idx.peVar ? PE_OV(idx.peVar) : null;
      peVal = Number.isFinite(ov) ? ov : "";
    }

    // 2) r_f & ERP*
    const { rf } = await getRF_forCountry(idx.country);
    const erpStar = erpMap[idx.country];

    // 3) 估值水平
    let level = "—";
    if (Number.isFinite(erpStar) && Number.isFinite(rf) && Number.isFinite(Number(peVal)) && Number(peVal) > 0) {
      const ep = 1/Number(peVal);
      const implied = ep - rf;
      if (implied >= erpStar + DELTA) level = "🟢 低估";
      else if (implied <= erpStar - DELTA) level = "🔴 高估";
      else level = "🟡 合理";
    } else if (!Number.isFinite(Number(peVal))) {
      level = "（待接入PE）";
    } else if (!Number.isFinite(erpStar)) {
      level = "（ERP*缺失）";
    } else if (!Number.isFinite(rf)) {
      level = "（r_f缺失）";
    }

    // 4) 加入表
    table.push([idx.name, peVal, level]);
  }

  // 写到 HS300 块下方两行（留空一行）
  const startRowIdx = startRow + 2;
  const range = `'${date}'!A${startRowIdx+1}:C${startRowIdx+table.length}`;
  await writeBlock(range, table);

  // 小表头加粗
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody:{ requests:[{
      repeatCell:{ range:{ sheetId, startRowIndex:startRowIdx, endRowIndex:startRowIdx+1, startColumnIndex:0, endColumnIndex:3 },
        cell:{ userEnteredFormat:{ textFormat:{ bold:true } } }, fields:"userEnteredFormat.textFormat" }
    }] }
  });
}

// ---------- 邮件（可选） ----------
async function maybeSendEmailBasic(payload) {
  const { SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_TO, MAIL_FROM_NAME } = process.env;
  if (!SMTP_HOST || !SMTP_USER || !SMTP_PASS || !MAIL_TO) return;
  const transporter = nodemailer.createTransport({
    host: SMTP_HOST, port:Number(SMTP_PORT||465), secure:Number(SMTP_PORT||465)===465,
    auth:{ user:SMTP_USER, pass:SMTP_PASS }
  });
  const pct = v => v==null? "-" : (v*100).toFixed(2)+"%";
  const html = `
  <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.6">
    <p>已更新：<b>${payload.date}</b></p>
    <ul>
      <li>P/E（TTM）：<b>${payload.pe ?? "-"}</b></li>
      <li>10Y名义：<b>${pct(payload.rf)}</b></li>
      <li>隐含ERP：<b>${pct(payload.impliedERP)}</b></li>
      <li>判定：<b>${payload.status}</b></li>
    </ul>
    <p><a target="_blank" href="https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/edit#gid=0">在线打开总表</a></p>
  </div>`;
  await transporter.sendMail({
    from:`"${MAIL_FROM_NAME || "Valuation Bot"}" <${SMTP_USER}>`,
    to:process.env.MAIL_TO, subject:`[估值] HS300（${payload.date}）— ${payload.status}`, html
  });
}

// ---------- Main ----------
(async () => {
  const date = todayStr(TZ);

  // 写入 HS300 详表（覆盖）
  const hsRows = await writeHS300Block();

  // 写入全市场指数估值列表（在其下方）
  await writeIndexSummary(hsRows);

  console.log("[DONE]", date);
})().catch(e => { console.error(e); process.exit(1); });
