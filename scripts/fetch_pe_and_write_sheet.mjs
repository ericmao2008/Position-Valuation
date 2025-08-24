// === Global Indices Summary (top) + HS300 + 6 Detailed Blocks ===
// 版式：
//   顶部：全市场指数（指数名称 | 当前PE | 估值水平 | 备注）
//   下面：HS300（详表）
//   再下面：SPX、NDX、DAX、N225、ASX200、NIFTY50 六个“沪深300同款”详表
//
// 可靠性：
// - PE：真实抓取（多源；SPX=multpl、N225=Nikkei、NDX/ASX200 可选 Playwright）；失败→ PE_OVERRIDE_*；
// - r_f：Investing.com 各国10Y；失败→ RF_*；
// - ERP*：Damodaran 解析，失败→ 内置国家ERP*兜底常量；
// - 计算只在合法数值时参与，格式化保证百分比/两位小数；
// - 整体顺序固定，确保格式正确；每次覆盖当日标签（不跳过）。

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
const tz = process.env.TZ || "Asia/Shanghai";
const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: tz }));
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
};
const USE_PLAYWRIGHT = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";

// 判定参数（HS300用；海外估值水平以各自国家ERP*与本国 r_f 判定）
const ERP_TARGET_CN = numOrDefault(process.env.ERP_TARGET, 0.0527);
const DELTA = numOrDefault(process.env.DELTA, 0.005);

// 兜底：HS300 PE/r_f
const PE_OVERRIDE_CN = (() => { const s=(process.env.PE_OVERRIDE??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)&&n>0&&n<1000?n:null; })();
const RF_OVERRIDE_CN = numOrDefault(process.env.RF_OVERRIDE, 0.0178);

// 兜底：海外国家 10Y（小数）
const RF_BACKUP = {
  USA:       numOrDefault(process.env.RF_US, 0.043),
  Germany:   numOrDefault(process.env.RF_DE, 0.023),
  Japan:     numOrDefault(process.env.RF_JP, 0.010),
  Australia: numOrDefault(process.env.RF_AU, 0.042),
  India:     numOrDefault(process.env.RF_IN, 0.071),
  Vietnam:   numOrDefault(process.env.RF_VN, 0.028),
  China:     RF_OVERRIDE_CN,
};
// 兜底：海外指数PE
const OV = k => { const s=(process.env[k]??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)&&n>0&&n<1000?n:null; };

// ---------- Google Sheets ----------
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if (!SPREADSHEET_ID) { console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL, null,
  (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version: "v4", auth });

async function ensureTodaySheet() {
  const title = todayStr();
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  let found = meta.data.sheets?.find(s => s.properties?.title === title);
  if (!found) {
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests:[{ addSheet:{ properties:{ title } } }] }
    });
    found = { properties: add.data?.replies?.[0]?.addSheet?.properties };
  }
  return { sheetTitle: title, sheetId: found.properties.sheetId };
}
async function valuesUpdate(rangeA1, rows) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID, range: rangeA1,
    valueInputOption:"USER_ENTERED",
    requestBody:{ values: rows }
  });
}
async function batchRequests(requests){ if(!requests?.length) return; await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody:{ requests } }); }

// ---------- Investing.com 10Y ----------
const INVESTING_10Y = {
  USA:       ["https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield","https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield"],
  Germany:   ["https://www.investing.com/rates-bonds/germany-10-year-bond-yield","https://cn.investing.com/rates-bonds/germany-10-year-bond-yield"],
  Japan:     ["https://www.investing.com/rates-bonds/japan-10-year-bond-yield","https://cn.investing.com/rates-bonds/japan-10-year-bond-yield"],
  Australia: ["https://www.investing.com/rates-bonds/australia-10-year-bond-yield","https://cn.investing.com/rates-bonds/australia-10-year-bond-yield"],
  India:     ["https://www.investing.com/rates-bonds/india-10-year-bond-yield","https://cn.investing.com/rates-bonds/india-10-year-bond-yield"],
  Vietnam:   ["https://www.investing.com/rates-bonds/vietnam-10-year-bond-yield","https://cn.investing.com/rates-bonds/vietnam-10-year-bond-yield"],
  China:     ["https://www.investing.com/rates-bonds/china-10-year-bond-yield","https://cn.investing.com/rates-bonds/china-10-year-bond-yield"],
};
async function fetchInvesting10Y(urls) {
  for (const url of urls) {
    try {
      const res = await fetch(url, { headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout: 12000 });
      if (!res.ok) continue;
      const html = await res.text();
      const m = html.match(/(\d+(?:\.\d+)?)\s*%/);
      if (m) { const v = Number(m[1])/100; if (Number.isFinite(v) && v>0 && v<1) return v; }
    } catch {}
  }
  return null;
}
async function rf(country) {
  const urls = INVESTING_10Y[country] || [];
  const real = await fetchInvesting10Y(urls);
  if (real != null) return { v: real, tag:"真实", link:`=HYPERLINK("${urls[0]}","Investing 10Y")` };
  return { v: RF_BACKUP[country], tag:"兜底", link:"—" };
}

// ---------- Damodaran ERP*（含内置兜底） ----------
const ERP_FALLBACK = {
  USA: 0.0527, Germany: 0.054, Japan: 0.056, Australia: 0.052, India: 0.060, Vietnam: 0.070, China: 0.0527
};
async function fetchERPMap() {
  const url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  const map = {};
  try {
    const res = await fetch(url, { headers:{ "User-Agent":UA }, timeout:15000 });
    if (!res.ok) throw new Error("Damodaran fetch not ok");
    const html = await res.text();
    const rows = html.split(/<\/tr>/i);
    for (const row of rows) {
      const text = row.replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim();
      if (!text) continue;
      const mCountry = text.match(/^([A-Za-z .&()-]+)\s/);
      const mERP = text.match(/(\d+(?:\.\d+)?)\s*%/);
      if (mCountry && mERP) {
        const c = mCountry[1].trim();
        const erp = Number(mERP[1])/100;
        if (Number.isFinite(erp)) map[c] = erp;
      }
    }
    if (map["United States"]) map["USA"] = map["United States"];
    // 合并兜底（缺哪个补哪个）
    for (const k of Object.keys(ERP_FALLBACK)) if (!Number.isFinite(map[k])) map[k]=ERP_FALLBACK[k];
    return map;
  } catch {
    return { ...ERP_FALLBACK }; // 全兜底
  }
}

// ---------- HS300：PE（Danjuan 多源 + Playwright 可选） ----------
async function pe_hs300() {
  // 1) JSON（两条）
  try {
    const r = await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=SH000300", { headers:{ "User-Agent":UA, "Referer":"https://danjuanfunds.com" }, timeout:12000 });
    if (r.ok) { const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm); if (Number.isFinite(v)&&v>0&&v<1000) return {v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")'} }
  } catch {}
  try {
    const r = await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail/SH000300", { headers:{ "User-Agent":UA, "Referer":"https://danjuanfunds.com" }, timeout:12000 });
    if (r.ok) { const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm); if (Number.isFinite(v)&&v>0&&v<1000) return {v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")'} }
  } catch {}
  // 2) HTML 内嵌 JSON
  try {
    const r = await fetch("https://danjuanfunds.com/index-detail/SH000300", { headers:{ "User-Agent":UA }, timeout:12000 });
    if (r.ok) {
      const html = await r.text();
      const m = html.match(/"pe_ttm"\s*:\s*"?([0-9]+(?:\.[0-9]+)?)"?/i);
      if (m){ const v=Number(m[1]); if (Number.isFinite(v)&&v>0&&v<1000) return {v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")'} }
    }
  } catch {}
  // 3) Playwright（可选）
  if (USE_PLAYWRIGHT) {
    try {
      const { chromium } = await import("playwright");
      const b = await chromium.launch({ headless:true }); const p = await b.newPage();
      p.setDefaultNavigationTimeout(15000); p.setDefaultTimeout(12000);
      await p.goto("https://danjuanfunds.com/index-detail/SH000300",{ waitUntil:"domcontentloaded" });
      let v = null;
      try {
        const resp = await p.waitForResponse(r=> r.url().includes("/djapi/index_evaluation/detail") && r.status()===200, { timeout:10000 });
        const data = await resp.json(); v=Number(data?.data?.pe_ttm ?? data?.data?.pe ?? data?.data?.valuation?.pe_ttm);
      } catch {}
      if (!Number.isFinite(v)) {
        const text = await p.locator("body").innerText();
        const m = text.match(/(PE|市盈率)[^0-9]{0,6}([0-9]+(?:\.[0-9]+)?)/i); if (m) v=Number(m[2]);
      }
      await b.close();
      if (Number.isFinite(v)&&v>0&&v<1000) return {v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")'};
    } catch {}
  }
  // 4) 兜底：历史 or override
  const vLast = await pe_last(); if (Number.isFinite(vLast)) return { v:vLast, tag:"兜底", link:"—" };
  const vOv = PE_OVERRIDE_CN; if (Number.isFinite(vOv)) return { v:vOv, tag:"兜底", link:"—" };
  return { v:"", tag:"", link:"—" };
}
async function pe_last(){
  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const titles = (meta.data.sheets||[]).map(s=>s.properties?.title).filter(t=>/^\d{4}-\d{2}-\d{2}$/.test(t)).sort();
    const last = titles[titles.length-1]; if(!last) return null;
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range:`'${last}'!B3:B3` });
    const v = Number(r.data.values?.[0]?.[0]); return Number.isFinite(v)? v : null;
  } catch { return null; }
}

// ---------- 海外指数 PE 抓取 ----------
// SPX=multpl
async function pe_spx(){
  try {
    const res = await fetch("https://www.multpl.com/s-p-500-pe-ratio", { headers:{ "User-Agent":UA }, timeout:15000 });
    if(res.ok){
      const html = await res.text();
      const m = html.match(/S&P 500 PE Ratio[^]*?([\d.]+)/i) || html.match(/current[^>]*>\s*([\d.]+)/i) || html.match(/(\d+(?:\.\d+)?)(?=\s*(?:x|$))/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:'=HYPERLINK("https://www.multpl.com/s-p-500-pe-ratio","Multpl")' }; }
    }
  } catch {}
  const ov = OV("PE_OVERRIDE_SPX"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}
// NDX=Nasdaq（Playwright兜底）
async function pe_ndx(){
  if (USE_PLAYWRIGHT) {
    try{
      const { chromium } = await import("playwright");
      const b=await chromium.launch({ headless:true }); const p=await b.newPage();
      p.setDefaultNavigationTimeout(15000); p.setDefaultTimeout(12000);
      await p.goto("https://www.nasdaq.com/market-activity/index/ndx", { waitUntil:"domcontentloaded" });
      const text = await p.locator("body").innerText();
      await b.close();
      const m = text.match(/P\/E\s*Ratio[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:'=HYPERLINK("https://www.nasdaq.com/market-activity/index/ndx","Nasdaq")' }; }
    }catch{}
  }
  const ov = OV("PE_OVERRIDE_NDX"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}
// DAX=（暂无稳接口，先兜底）
async function pe_dax(){ const ov=OV("PE_OVERRIDE_DAX"); return { v: ov??"", tag: ov?"兜底":"", link:"—" }; }
// N225=Nikkei 官方 PER
async function pe_n225(){
  try{
    const res = await fetch("https://indexes.nikkei.co.jp/en/nkave", { headers:{ "User-Agent":UA }, timeout:15000 });
    if(res.ok){
      const html = await res.text();
      const m = html.match(/PER[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:'=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave","Nikkei")' }; }
    }
  }catch{}
  const ov = OV("PE_OVERRIDE_N225"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}
// ASX200=S&P DJI（Playwright兜底）
async function pe_asx200(){
  if(USE_PLAYWRIGHT){
    try{
      const { chromium } = await import("playwright");
      const b=await chromium.launch({ headless:true }); const p=await b.newPage();
      p.setDefaultNavigationTimeout(15000); p.setDefaultTimeout(12000);
      await p.goto("https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview", { waitUntil:"domcontentloaded" });
      const text = await p.locator("body").innerText();
      await b.close();
      const m = text.match(/P\/E\s*(?:Ratio)?[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:'=HYPERLINK("https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview","S&P DJI")' }; }
    }catch{}
  }
  const ov = OV("PE_OVERRIDE_ASX200"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}
// NIFTY50=NSE India API
async function pe_nifty50(){
  try{
    const res = await fetch("https://www.nseindia.com/api/allIndices", { headers:{ "User-Agent":UA, "Referer":"https://www.nseindia.com/" }, timeout:15000 });
    if(res.ok){
      const j=await res.json();
      const row=(j?.data||[]).find(r => (r?.index||"").toUpperCase().includes("NIFTY 50"));
      const v=Number(row?.pe);
      if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:'=HYPERLINK("https://www.nseindia.com/","NSE India API")' };
    }
  }catch{}
  const ov = OV("PE_OVERRIDE_NIFTY50"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}

// ---------- 写顶部「全市场指数」总览 ----------
async function writeGlobalSummary(erpMap){
  const { sheetTitle, sheetId } = await ensureTodaySheet();
  const list = [
    { title:"标普500",   country:"USA",       fn:pe_spx },
    { title:"纳斯达克100", country:"USA",     fn:pe_ndx },
    { title:"德国DAX",   country:"Germany",   fn:pe_dax },
    { title:"日经225",   country:"Japan",     fn:pe_n225 },
    { title:"澳洲ASX200", country:"Australia",fn:pe_asx200 },
    { title:"印度Nifty50",country:"India",    fn:pe_nifty50 },
  ];

  const rows = [["指数名称","当前PE","估值水平","备注"]];
  for (const it of list){
    const peRes = await it.fn(); const pe = peRes.v;
    const { v:rfV } = await rf(it.country);
    const erpStar = erpMap?.[it.country];

    let level="—", note="";
    if (Number.isFinite(Number(pe)) && Number.isFinite(rfV) && Number.isFinite(erpStar)) {
      const ep = 1/Number(pe); const implied = ep - rfV;
      if (implied >= erpStar + DELTA) level = "🟢 低估";
      else if (implied <= erpStar - DELTA) level = "🔴 高估";
      else level = "🟡 合理";
    } else {
      if (!Number.isFinite(Number(pe))) note = "（PE待接入/兜底）";
      else if (!Number.isFinite(erpStar)) note = "（ERP*缺失）";
      else if (!Number.isFinite(rfV)) note = "（r_f缺失）";
    }
    rows.push([it.title, Number.isFinite(Number(pe))? Number(pe):"", level, note]);
  }

  await valuesUpdate(`'${sheetTitle}'!A1:D${rows.length}`, rows);
  await batchRequests([
    { repeatCell:{ range:{ sheetId, startRowIndex:0, endRowIndex:1, startColumnIndex:0, endColumnIndex:4 },
      cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
      fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:0, endIndex:1 }, properties:{ pixelSize:180 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:1, endIndex:2 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:2, endIndex:3 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:3, endIndex:4 }, properties:{ pixelSize:220 }, fields:"pixelSize" } },
  ]);

  return rows.length + 1; // 返回下一块起始行（空一行）
}

// ---------- 写“沪深300”详表 ----------
async function writeHS300Block(startRow){
  const { sheetTitle, sheetId } = await ensureTodaySheet();

  const peRes = await pe_hs300();
  const { v:rfV, tag:rfTag, link:rfLink } = await rf("China");

  const pe = peRes.v; const peTag = peRes.tag || (pe!=""?"真实":"");
  const ep = Number.isFinite(pe)? 1/pe : null;
  const impliedERP = (ep!=null && Number.isFinite(rfV)) ? (ep - rfV) : null;
  const peLimit = (Number.isFinite(rfV)) ? Number((1/(rfV + ERP_TARGET_CN)).toFixed(2)) : null;
  let status="需手动更新";
  if (impliedERP!=null) {
    if (impliedERP >= ERP_TARGET_CN + DELTA) status = "🟢 买点（低估）";
    else if (impliedERP <= ERP_TARGET_CN - DELTA) status = "🔴 卖点（高估）";
    else status = "🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数","沪深300","真实","宽基指数估值分块", '=HYPERLINK("https://www.csindex.com.cn/zh-CN/indices/index-detail/000300","中证指数有限公司")'],
    ["P/E（TTM）", pe ?? "", peTag, "蛋卷基金 index-detail（JSON→HTML）", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", pe!=""? "真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rfV ?? "", rfTag, "Investing.com 中国10年期国债收益率", rfLink],
    ["隐含ERP = E/P − r_f", impliedERP ?? "", impliedERP!=null? "真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", ERP_TARGET_CN, "真实", "建议参考达摩达兰", '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", peLimit!=null? "真实":"兜底", "直观对照","—"],
    ["判定", status, impliedERP!=null? "真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];
  const endRow = startRow + rows.length - 1;

  await valuesUpdate(`'${sheetTitle}'!A${startRow}:E${endRow}`, rows);
  await batchRequests([
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow-1, endRowIndex:startRow, startColumnIndex:0, endColumnIndex:5 },
      cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
      fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:0, endIndex:1 }, properties:{ pixelSize:140 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:1, endIndex:2 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:2, endIndex:3 }, properties:{ pixelSize:80  }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:3, endIndex:4 }, properties:{ pixelSize:420 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:4, endIndex:5 }, properties:{ pixelSize:260 }, fields:"pixelSize" } },
    // B列格式
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+1, endRowIndex:startRow+2, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } }, // P/E
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+2, endRowIndex:startRow+7, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } }, fields:"userEnteredFormat.numberFormat" } }, // E/P~δ
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+7, endRowIndex:startRow+8, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
    // “数据”列居中
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow, startColumnIndex:2, endColumnIndex:3 }, cell:{ userEnteredFormat:{ horizontalAlignment:"CENTER" } }, fields:"userEnteredFormat.horizontalAlignment" } }
  ]);

  return endRow + 2; // 下一块起点（空一行）
}

// ---------- 写海外指数详表（与 HS300 同款） ----------
async function writeDetailBlock(startRow, cfg){
  const { sheetTitle, sheetId } = await ensureTodaySheet();
  const peRes = await cfg.pe();
  const rfRes = await rf(cfg.country);
  const erpMap = await fetchERPMap();
  const erpStar = erpMap?.[cfg.country];

  const pe = peRes.v; const peTag = peRes.tag || (pe!=""? "真实":"");
  const rfV = rfRes.v; const rfTag = rfRes.tag || (rfV!=null? "真实":"");
  const ep = Number.isFinite(pe)? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rfV)) ? (ep - rfV) : null;

  let status = "需手动更新";
  if (implied!=null && Number.isFinite(erpStar)) {
    if (implied >= erpStar + DELTA) status = "🟢 买点（低估）";
    else if (implied <= erpStar - DELTA) status = "🔴 卖点（高估）";
    else status = "🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数", cfg.title, "真实", "宽基指数估值分块", cfg.home || "—"],
    ["P/E（TTM）", pe ?? "", peTag, cfg.peDesc || "—", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", pe!=""? "真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rfV ?? "", rfTag, "Investing.com 10Y", rfRes.link || "—"],
    ["隐含ERP = E/P − r_f", implied ?? "", implied!=null? "真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", Number.isFinite(erpStar)? erpStar:"", Number.isFinite(erpStar)?"真实":"兜底", "达摩达兰国家表", '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", (Number.isFinite(rfV)&&Number.isFinite(erpStar))? Number((1/(rfV+erpStar)).toFixed(2)):"", (Number.isFinite(rfV)&&Number.isFinite(erpStar))?"真实":"兜底", "直观对照","—"],
    ["判定", status, (implied!=null&&Number.isFinite(erpStar))? "真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];
  const endRow = startRow + rows.length - 1;

  await valuesUpdate(`'${sheetTitle}'!A${startRow}:E${endRow}`, rows);
  await batchRequests([
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow-1, endRowIndex:startRow, startColumnIndex:0, endColumnIndex:5 },
      cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
      fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:0, endIndex:1 }, properties:{ pixelSize:140 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:1, endIndex:2 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:2, endIndex:3 }, properties:{ pixelSize:80  }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:3, endIndex:4 }, properties:{ pixelSize:420 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:4, endIndex:5 }, properties:{ pixelSize:260 }, fields:"pixelSize" } },
    // B列格式
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+1, endRowIndex:startRow+2, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+2, endRowIndex:startRow+7, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } }, fields:"userEnteredFormat.numberFormat" } },
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+7, endRowIndex:startRow+8, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
    // “数据”列居中
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow, startColumnIndex:2, endColumnIndex:3 }, cell:{ userEnteredFormat:{ horizontalAlignment:"CENTER" } }, fields:"userEnteredFormat.horizontalAlignment" } }
  ]);

  return endRow + 2;
}

// ---------- 顶部总览 + HS300 + 六个分块（严格顺序） ----------
async function main() {
  const erpMap = await fetchERPMap(); // 带兜底
  // 顶部总览
  const startNext = await writeGlobalSummary(erpMap);
  // HS300
  let row = await writeHS300Block(startNext);
  // 六个分块
  row = await writeDetailBlock(row, { title:"标普500", country:"USA",       pe:pe_spx,     home:'=HYPERLINK("https://www.spglobal.com/spdji/en/indices/equity/sp-500/#overview","S&P DJI")', peDesc:"Multpl（S&P500 TTM PE）" });
  row = await writeDetailBlock(row, { title:"纳斯达克100", country:"USA",    pe:pe_ndx,     home:'=HYPERLINK("https://www.nasdaq.com/market-activity/index/ndx","Nasdaq")', peDesc:"Nasdaq 指数页（P/E Ratio）" });
  row = await writeDetailBlock(row, { title:"德国DAX",   country:"Germany",  pe:pe_dax,     home:'=HYPERLINK("https://www.deutsche-boerse.com/dbg-en/","Deutsche Börse")', peDesc:"（暂用兜底 PE_OVERRIDE_DAX）" });
  row = await writeDetailBlock(row, { title:"日经225",   country:"Japan",    pe:pe_n225,    home:'=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave","Nikkei")', peDesc:"Nikkei 官方 PER" });
  row = await writeDetailBlock(row, { title:"澳洲ASX200",country:"Australia",pe:pe_asx200,  home:'=HYPERLINK("https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview","S&P DJI")', peDesc:"S&P DJI 指数页（P/E）" });
  row = await writeDetailBlock(row, { title:"印度Nifty50",country:"India",   pe:pe_nifty50, home:'=HYPERLINK("https://www.nseindia.com/","NSE India")', peDesc:"NSE India API（/api/allIndices）" });

  console.log("[DONE]", todayStr());
}

// ====== Run ======
main().catch(e => { console.error(e); process.exit(1); });
