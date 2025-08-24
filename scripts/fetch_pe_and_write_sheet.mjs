// === Global Indices on Top + 6 Detailed Blocks (SPX, NDX, DAX, N225, ASX200, NIFTY50) ===
// - 顶部：全市场指数（指数名称 | 当前PE | 估值水平）
// - 下面依次输出 6 个“沪深300同款”分块（带“数据=真实/兜底”、百分比样式、表头灰底、判定 emoji）
// - 数据源：PE=各指数权威页(见各函数；失败→ PE_OVERRIDE_* 兜底)；r_f=Investing.com 各国10Y（失败→ RF_* 兜底）；ERP*=Damodaran 国家表
// - 每次运行覆盖当日 tab，不跳过；邮件逻辑保持精简可选

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
const TZ = process.env.TZ || "Asia/Shanghai";
const DELTA = numOrDefault(process.env.DELTA, 0.005); // 0.50%
const USE_PLAYWRIGHT = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";

// 兜底：各国 rf（小数），Investing 抓不到时使用
const RF_BACKUP = {
  USA:       numOrDefault(process.env.RF_US, 0.043),
  Germany:   numOrDefault(process.env.RF_DE, 0.023),
  Japan:     numOrDefault(process.env.RF_JP, 0.010),
  Australia: numOrDefault(process.env.RF_AU, 0.042),
  India:     numOrDefault(process.env.RF_IN, 0.071),
  Vietnam:   numOrDefault(process.env.RF_VN, 0.028),
};
// 兜底：各指数 PE
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
async function ensureToday() {
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
  return { sheetTitle: title, sheetId: found.properties.sheetId };
}
async function write(rangeA1, rows) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID, range: rangeA1,
    valueInputOption:"USER_ENTERED",
    requestBody:{ values: rows }
  });
}
async function formatHeader(sheetId, startRow=0, endRow=1, colEnd=5) {
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody:{ requests:[{
      repeatCell:{
        range:{ sheetId, startRowIndex:startRow, endRowIndex:endRow, startColumnIndex:0, endColumnIndex:colEnd },
        cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
        fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
      }
    }] }
  });
}
async function setWidths(sheetId, defs) {
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody:{ requests: defs.map(({start,end,px}) => ({
      updateDimensionProperties: {
        range:{ sheetId, dimension:"COLUMNS", startIndex:start, endIndex:end },
        properties:{ pixelSize:px }, fields:"pixelSize"
      }
    })) }
  });
}
async function formatB(sheetId, row0, pattern){
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody:{ requests:[{
      repeatCell:{ range:{ sheetId, startRowIndex:row0, endRowIndex:row0+1, startColumnIndex:1, endColumnIndex:2 },
        cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern } } }, fields:"userEnteredFormat.numberFormat" }
    }] }
  });
}

// ---------- Investing.com 10Y ----------
const INVESTING_10Y = {
  USA:       ["https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield","https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield"],
  Germany:   ["https://www.investing.com/rates-bonds/germany-10-year-bond-yield","https://cn.investing.com/rates-bonds/germany-10-year-bond-yield"],
  Japan:     ["https://www.investing.com/rates-bonds/japan-10-year-bond-yield","https://cn.investing.com/rates-bonds/japan-10-year-bond-yield"],
  Australia: ["https://www.investing.com/rates-bonds/australia-10-year-bond-yield","https://cn.investing.com/rates-bonds/australia-10-year-bond-yield"],
  India:     ["https://www.investing.com/rates-bonds/india-10-year-bond-yield","https://cn.investing.com/rates-bonds/india-10-year-bond-yield"],
  Vietnam:   ["https://www.investing.com/rates-bonds/vietnam-10-year-bond-yield","https://cn.investing.com/rates-bonds/vietnam-10-year-bond-yield"],
};
async function fetchInvesting10Y(urls) {
  for (const url of urls) {
    try {
      const res = await fetch(url, { headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:10000 });
      if (!res.ok) continue;
      const html = await res.text();
      const m = html.match(/(\d+(?:\.\d+)?)\s*%/);
      if (m) { const v=Number(m[1])/100; if (Number.isFinite(v) && v>0 && v<1) return v; }
    } catch {}
  }
  return null;
}
async function rf(country){
  const urls = INVESTING_10Y[country]||[];
  const real = await fetchInvesting10Y(urls);
  if (real != null) return { v: real, tag: "真实", src: `=HYPERLINK("${urls[0]}","Investing 10Y")` };
  return { v: RF_BACKUP[country], tag:"兜底", src:"—" };
}

// ---------- Damodaran ERP* ----------
async function fetchERPMap() {
  const url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  const map = {};
  try{
    const res = await fetch(url, { headers:{ "User-Agent":UA }, timeout:12000 });
    if(!res.ok) return null;
    const html = await res.text();
    const trs = html.split(/<\/tr>/i);
    for (const tr of trs){
      const text = tr.replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim();
      if(!text) continue;
      const mc = text.match(/^([A-Za-z .&()-]+)\s/);
      const me = text.match(/(\d+(?:\.\d+)?)\s*%/);
      if(mc && me){
        const country = mc[1].trim();
        const erp = Number(me[1])/100;
        if(Number.isFinite(erp)) map[country]=erp;
      }
    }
    if(map["United States"]) map["USA"]=map["United States"];
    return map;
  }catch{ return null; }
}

// ---------- 各指数 PE 真实抓取 ----------
// 1) S&P 500（multpl）
async function pe_spx(){
  try{
    const res = await fetch("https://www.multpl.com/s-p-500-pe-ratio", { headers:{ "User-Agent":UA }, timeout:10000 });
    if(res.ok){
      const html = await res.text();
      const m = html.match(/S&P 500 PE Ratio[^]*?([\d.]+)/i) || html.match(/current[^>]*>\s*([\d.]+)/i) || html.match(/(\d+(?:\.\d+)?)(?=\s*(?:x|$))/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return {v, tag:"真实", src:'=HYPERLINK("https://www.multpl.com/s-p-500-pe-ratio","Multpl")'} }
    }
  }catch{}
  const ov = OV("PE_OVERRIDE_SPX"); return { v: ov, tag: ov? "兜底":"", src: ov? "—":"" };
}
// 2) Nasdaq-100（Nasdaq 指数页，Playwright 兜底）
async function pe_ndx(){
  if(USE_PLAYWRIGHT){
    try{
      const { chromium } = await import("playwright");
      const b = await chromium.launch({ headless:true }); const p = await b.newPage();
      p.setDefaultNavigationTimeout(12000); p.setDefaultTimeout(10000);
      await p.goto("https://www.nasdaq.com/market-activity/index/ndx", { waitUntil:"domcontentloaded" });
      const text = await p.locator("body").innerText();
      await b.close();
      const m = text.match(/P\/E\s*Ratio[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return {v, tag:"真实", src:'=HYPERLINK("https://www.nasdaq.com/market-activity/index/ndx","Nasdaq")'} }
    }catch{}
  }
  const ov = OV("PE_OVERRIDE_NDX"); return { v: ov, tag: ov? "兜底":"", src: ov? "—":"" };
}
// 3) DAX（德交所/Xetra 官方页面结构多变，先用兜底变量）
async function pe_dax(){
  const ov = OV("PE_OVERRIDE_DAX"); return { v: ov, tag: ov? "兜底":"", src: ov? "—":"" };
}
// 4) Nikkei 225（Nikkei 官方 PER）
async function pe_n225(){
  try{
    const res = await fetch("https://indexes.nikkei.co.jp/en/nkave", { headers:{ "User-Agent":UA }, timeout:10000 });
    if(res.ok){
      const html = await res.text();
      const m = html.match(/PER[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return {v, tag:"真实", src:'=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave","Nikkei")'} }
    }
  }catch{}
  const ov = OV("PE_OVERRIDE_N225"); return { v: ov, tag: ov? "兜底":"", src: ov? "—":"" };
}
// 5) ASX200（S&P DJI 页面，Playwright 兜底）
async function pe_asx200(){
  if(USE_PLAYWRIGHT){
    try{
      const { chromium } = await import("playwright");
      const b=await chromium.launch({ headless:true }); const p=await b.newPage();
      p.setDefaultNavigationTimeout(12000); p.setDefaultTimeout(10000);
      await p.goto("https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview", { waitUntil:"domcontentloaded" });
      const text = await p.locator("body").innerText();
      await b.close();
      const m = text.match(/P\/E\s*(?:Ratio)?[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return {v, tag:"真实", src:'=HYPERLINK("https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview","S&P DJI")'} }
    }catch{}
  }
  const ov = OV("PE_OVERRIDE_ASX200"); return { v: ov, tag: ov? "兜底":"", src: ov? "—":"" };
}
// 6) Nifty50（NSE India API）
async function pe_nifty50(){
  try{
    const res = await fetch("https://www.nseindia.com/api/allIndices", { headers:{ "User-Agent":UA, "Referer":"https://www.nseindia.com/" }, timeout:12000 });
    if(res.ok){
      const j = await res.json();
      const row = (j?.data||[]).find(r => (r?.index||"").toUpperCase().includes("NIFTY 50"));
      const v = Number(row?.pe);
      if(Number.isFinite(v)&&v>0&&v<1000) return {v, tag:"真实", src:'=HYPERLINK("https://www.nseindia.com/","NSE India API")'};
    }
  }catch{}
  const ov = OV("PE_OVERRIDE_NIFTY50"); return { v: ov, tag: ov? "兜底":"", src: ov? "—":"" };
}

// ---------- 写「全市场指数」总览（顶部） ----------
async function writeGlobalSummary(erpMap, topRow=1){
  const { sheetTitle, sheetId } = await ensureToday();
  const list = [
    { name:"标普500",      key:"SPX",     country:"USA",       peFn:pe_spx },
    { name:"纳斯达克100",  key:"NDX",     country:"USA",       peFn:pe_ndx },
    { name:"德国DAX",      key:"DAX",     country:"Germany",   peFn:pe_dax },
    { name:"日经225",      key:"N225",    country:"Japan",     peFn:pe_n225 },
    { name:"澳洲ASX200",   key:"ASX200",  country:"Australia", peFn:pe_asx200 },
    { name:"印度Nifty50",  key:"NIFTY50", country:"India",     peFn:pe_nifty50 },
  ];

  const rows = [["指数名称","当前PE","估值水平","备注"]];
  for (const it of list){
    const peRes = await it.peFn();
    const pe = peRes.v;
    const rfRes = await rf(it.country);
    const erpStar = erpMap?.[it.country];

    let level="—", note="";
    if (Number.isFinite(Number(pe)) && Number.isFinite(rfRes.v) && Number.isFinite(erpStar)) {
      const ep = 1/Number(pe);
      const implied = ep - rfRes.v;
      if (implied >= erpStar + DELTA) level = "🟢 低估";
      else if (implied <= erpStar - DELTA) level = "🔴 高估";
      else level = "🟡 合理";
    } else {
      if (!Number.isFinite(Number(pe))) note = "（PE待接入/兜底）";
      else if (!Number.isFinite(rfRes.v)) note = "（r_f缺失）";
      else if (!Number.isFinite(erpStar)) note = "（ERP*缺失）";
    }
    rows.push([it.name, pe ?? "", level, note]);
  }

  // 顶部 A1:D? 写入全市场表
  const endRow = topRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${topRow}:D${endRow}`, rows);
  // 加粗表头、列宽
  await formatHeader(sheetId, topRow-1, topRow, 4);
  await setWidths(sheetId, [
    { start:0, end:1, px:180 },
    { start:1, end:2, px:120 },
    { start:2, end:3, px:120 },
    { start:3, end:4, px:220 },
  ]);

  return endRow + 2; // 返回下一块开始的行号（空1行）
}

// ---------- 写一个指数的“详细分块”（与沪深300同款） ----------
async function writeDetailBlock(startRow, title, peRes, rfRes, erpStar, sourceLinks){
  const { sheetTitle, sheetId } = await ensureToday();

  const pe = peRes.v; const peTag = peRes.tag || (Number.isFinite(pe) ? "真实":"");
  const rf = rfRes.v; const rfTag = rfRes.tag || (Number.isFinite(rf) ? "真实":"");

  const ep = Number.isFinite(pe)? 1/pe : null;
  const impliedERP = (ep!=null && Number.isFinite(rf)) ? (ep - rf) : null;
  const peLimit = (Number.isFinite(rf) && Number.isFinite(erpStar)) ? Number((1/(rf + erpStar)).toFixed(2)) : null;

  let status="需手动更新";
  if (impliedERP!=null && Number.isFinite(erpStar)) {
    if (impliedERP >= erpStar + DELTA) status = "🟢 买点（低估）";
    else if (impliedERP <= erpStar - DELTA) status = "🔴 卖点（高估）";
    else status = "🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数", title, "真实", "宽基指数估值分块", sourceLinks?.home || "—"],
    ["P/E（TTM）", pe ?? "", peTag || (pe!=null? "真实":"兜底"), sourceLinks?.peDesc || "—", sourceLinks?.peLink || "—"],
    ["E/P = 1 / P/E", ep ?? "", pe!=null? "真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rfTag || (rf!=null? "真实":"兜底"), "Investing.com 10Y", rfRes.src || "—"],
    ["隐含ERP = E/P − r_f", impliedERP ?? "", impliedERP!=null? "真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", erpStar ?? "", "真实", "达摩达兰国家风险溢价", '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", peLimit!=null? "真实":"兜底", "直观对照","—"],
    ["判定", status, impliedERP!=null? "真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];

  const endRow = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${endRow}`, rows);

  // 样式：表头、列宽、B列数值格式、C列居中
  await formatHeader(sheetId, startRow-1, startRow, 5);
  await setWidths(sheetId, [
    { start:0, end:1, px:140 }, { start:1, end:2, px:120 }, { start:2, end:3, px:80  },
    { start:3, end:4, px:420 }, { start:4, end:5, px:260 }
  ]);
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody:{ requests:[
      // P/E 两位小数
      { repeatCell:{ range:{ sheetId, startRowIndex:startRow-1+2, endRowIndex:startRow-1+3, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
      // E/P, r_f, 隐含ERP, ERP*, δ 百分比
      { repeatCell:{ range:{ sheetId, startRowIndex:startRow-1+3, endRowIndex:startRow-1+8, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } }, fields:"userEnteredFormat.numberFormat" } },
      // P/E上限 两位小数
      { repeatCell:{ range:{ sheetId, startRowIndex:startRow-1+8, endRowIndex:startRow-1+9, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
      // “数据”列居中
      { repeatCell:{ range:{ sheetId, startRowIndex:startRow-1+1, startColumnIndex:2, endColumnIndex:3 }, cell:{ userEnteredFormat:{ horizontalAlignment:"CENTER" } }, fields:"userEnteredFormat.horizontalAlignment" } }
    ] }
  });

  return endRow + 2; // 下一块起始行（空一行）
}

// ---------- Main ----------
(async () => {
  const { sheetTitle } = await ensureToday();
  const erpMap = await fetchERPMap() || {};

  // 顶部：全市场指数总览
  const nextRow = await writeGlobalSummary(erpMap, 1);

  // 逐个输出详细分块（按你指定的顺序）：
  // 标普500 → 纳斯达克100 → 德国DAX → 日经225 → 澳洲ASX200 → 印度Nifty50
  let row = nextRow;

  // 1) S&P 500
  const pe_spx_res = await pe_spx();
  const rf_spx = await rf("USA");
  row = await writeDetailBlock(row, "标普500", pe_spx_res, rf_spx, erpMap?.USA, {
    home: '=HYPERLINK("https://www.spglobal.com/spdji/en/indices/equity/sp-500/#overview","S&P DJI")',
    peDesc: "Multpl（S&P500 TTM PE）", peLink: '=HYPERLINK("https://www.multpl.com/s-p-500-pe-ratio","Multpl")'
  });

  // 2) Nasdaq-100
  const pe_ndx_res = await pe_ndx();
  const rf_ndx = await rf("USA");
  row = await writeDetailBlock(row, "纳斯达克100", pe_ndx_res, rf_ndx, erpMap?.USA, {
    home: '=HYPERLINK("https://www.nasdaq.com/market-activity/index/ndx","Nasdaq")',
    peDesc: "Nasdaq 指数页（P/E Ratio）", peLink: '=HYPERLINK("https://www.nasdaq.com/market-activity/index/ndx","Nasdaq")'
  });

  // 3) DAX
  const pe_dax_res = await pe_dax();
  const rf_dax = await rf("Germany");
  row = await writeDetailBlock(row, "德国DAX", pe_dax_res, rf_dax, erpMap?.Germany, {
    home: '=HYPERLINK("https://www.deutsche-boerse.com/dbg-en/","Deutsche Börse")',
    peDesc: "（暂用兜底 PE_OVERRIDE_DAX）", peLink: "—"
  });

  // 4) Nikkei 225
  const pe_n225_res = await pe_n225();
  const rf_n225 = await rf("Japan");
  row = await writeDetailBlock(row, "日经225", pe_n225_res, rf_n225, erpMap?.Japan, {
    home: '=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave","Nikkei")',
    peDesc: "Nikkei 官方 PER", peLink: '=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave","Nikkei")'
  });

  // 5) ASX200
  const pe_asx_res = await pe_asx200();
  const rf_asx = await rf("Australia");
  row = await writeDetailBlock(row, "澳洲ASX200", pe_asx_res, rf_asx, erpMap?.Australia, {
    home: '=HYPERLINK("https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview","S&P DJI")',
    peDesc: "S&P DJI 指数页（P/E）", peLink: '=HYPERLINK("https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview","S&P DJI")'
  });

  // 6) Nifty50
  const pe_nifty_res = await pe_nifty50();
  const rf_in = await rf("India");
  row = await writeDetailBlock(row, "印度Nifty50", pe_nifty_res, rf_in, erpMap?.India, {
    home: '=HYPERLINK("https://www.nseindia.com/","NSE India")',
    peDesc: "NSE India API（/api/allIndices）", peLink: '=HYPERLINK("https://www.nseindia.com/","NSE India")'
  });

  console.log("[DONE]", sheetTitle);
})();
