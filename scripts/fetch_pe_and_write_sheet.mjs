// === Global Summary (top) + HS300 + SPX + NDX + DAX + N225 + ASX200 + NIFTY50 + CSIH30533 ===
// - 顶部：全市场指数（指数名称 | 当前PE | 估值水平 | 备注）
// - 下方：HS300 详表；再依次 7 个分块（与 HS300 同款结构：字段 | 数值 | 数据 | 说明 | 数据源）
// - PE 来源（按你指定）：
//   SPX:  https://danjuanfunds.com/dj-valuation-table-detail/SP500
//   NDX:  https://danjuanfunds.com/dj-valuation-table-detail/NDX
//   DAX:  https://finance.yahoo.com/quote/DAX/
//   N225: https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per
//   ASX:  https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview
//   NIFTY:https://trendlyne.com/equity/PE/NIFTY/1887/nifty-50-price-to-earning-ratios/
//   CSIH30533: https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533
//   HS300: 蛋卷 JSON→HTML
// - r_f: HS300→China 10Y；SPX/NDX→US 10Y；DAX→DE 10Y；N225→JP 10Y；ASX→AU 10Y；NIFTY→IN 10Y；CSIH30533→US 10Y；Investing.com 抓取，失败→ RF_* 兜底
// - ERP*: Damodaran 解析，失败→ 内置兜底
// - 计算仅在合法数值参与；数据列写“真实/兜底”；样式与百分比/两位小数保持一致
// - 每次覆盖当日标签，不跳过

import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const tz = process.env.TZ || "Asia/Shanghai";
const USE_PLAYWRIGHT = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";

const numOrDefault = (v, d) => { if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; };
const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US",{timeZone:tz}));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const strip = (html) => html.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");

// 判定参数
const ERP_TARGET_CN = numOrDefault(process.env.ERP_TARGET, 0.0527); // HS300
const DELTA = numOrDefault(process.env.DELTA, 0.005);

// 兜底 PE（小数）
const OV = k => { const s=(process.env[k]??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)&&n>0&&n<1000?n:null; };

// 兜底 r_f（小数）
const RF_CN = numOrDefault(process.env.RF_OVERRIDE, 0.0178);
const RF_US = numOrDefault(process.env.RF_US,       0.0425);
const RF_DE = numOrDefault(process.env.RF_DE,       0.0230);
const RF_JP = numOrDefault(process.env.RF_JP,       0.0100);
const RF_AU = numOrDefault(process.env.RF_AU,       0.0420);
const RF_IN = numOrDefault(process.env.RF_IN,       0.0710);
const RF_VN = numOrDefault(process.env.RF_VN,       0.0280);

// Google Sheets
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if(!SPREADSHEET_ID){ console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(process.env.GOOGLE_CLIENT_EMAIL, null, (process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]);
const sheets = google.sheets({ version:"v4", auth });

async function ensureToday() {
  const title = todayStr();
  const meta = await sheets.spreadsheets.get({ spreadsheetId:SPREADSHEET_ID });
  let found = meta.data.sheets?.find(s => s.properties?.title===title);
  if(!found){
    const add = await sheets.spreadsheets.batchUpdate({ spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title }}}]} });
    found = { properties: add.data?.replies?.[0]?.addSheet?.properties };
  }
  return { sheetTitle:title, sheetId:found.properties.sheetId };
}
async function write(range, rows){
  await sheets.spreadsheets.values.update({ spreadsheetId:SPREADSHEET_ID, range, valueInputOption:"USER_ENTERED", requestBody:{ values: rows }});
}
async function batch(reqs){ if(!reqs?.length) return; await sheets.spreadsheets.batchUpdate({ spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:reqs } }); }

// Investing.com 10Y（按国家）
const INVEST_URL = {
  China:     ["https://cn.investing.com/rates-bonds/china-10-year-bond-yield","https://www.investing.com/rates-bonds/china-10-year-bond-yield"],
  USA:       ["https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield","https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"],
  Germany:   ["https://www.investing.com/rates-bonds/germany-10-year-bond-yield","https://cn.investing.com/rates-bonds/germany-10-year-bond-yield"],
  Japan:     ["https://www.investing.com/rates-bonds/japan-10-year-bond-yield","https://cn.investing.com/rates-bonds/japan-10-year-bond-yield"],
  Australia: ["https://www.investing.com/rates-bonds/australia-10-year-bond-yield","https://cn.investing.com/rates-bonds/australia-10-year-bond-yield"],
  India:     ["https://www.investing.com/rates-bonds/india-10-year-bond-yield","https://cn.investing.com/rates-bonds/india-10-year-bond-yield"],
  Vietnam:   ["https://www.investing.com/rates-bonds/vietnam-10-year-bond-yield","https://cn.investing.com/rates-bonds/vietnam-10-year-bond-yield"],
};
const RF_FALLBACK = { China:RF_CN, USA:RF_US, Germany:RF_DE, Japan:RF_JP, Australia:RF_AU, India:RF_IN, Vietnam:RF_VN };

async function rf(country){
  const urls = INVEST_URL[country] || [];
  for(const url of urls){
    try{
      const r = await fetch(url, { headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:15000 });
      if(!r.ok) continue;
      const html = await r.text();
      const m = html.match(/(\d+(?:\.\d+)?)\s*%/);
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","Investing 10Y")` }; }
    }catch{}
  }
  return { v: RF_FALLBACK[country], tag:"兜底", link:"—" };
}

// Damodaran ERP*（含兜底）
const ERP_FALLBACK = { USA:0.0527, Germany:0.0540, Japan:0.0560, Australia:0.0520, India:0.0600, China:0.0527, Vietnam:0.0700 };
async function fetchERPMap(){
  const url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  const map = {};
  try{
    const res = await fetch(url, { headers:{ "User-Agent":UA }, timeout:20000 });
    if(!res.ok) throw new Error("damodaran not ok");
    const html = await res.text();
    const rows = html.split(/<\/tr>/i);
    for(const row of rows){
      const text = row.replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim();
      if(!text) continue;
      const mC = text.match(/^([A-Za-z .&()-]+)\s/);
      const mE = text.match(/(\d+(?:\.\d+)?)\s*%/);
      if(mC&&mE){
        const c = mC[1].trim();
        const e = Number(mE[1])/100;
        if(Number.isFinite(e)) map[c]=e;
      }
    }
    if(map["United States"]) map["USA"]=map["United States"];
  }catch{
    // ignore
  }
  // 合并兜底
  for(const k of Object.keys(ERP_FALLBACK)) if(!Number.isFinite(map[k])) map[k]=ERP_FALLBACK[k];
  return map;
}

// —— 按你指定的网址写的 PE 抓取器 ——

// HS300：蛋卷 JSON→HTML
async function pe_hs300(){
  const url1="https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=SH000300";
  const url1b="https://danjuanfunds.com/djapi/index_evaluation/detail/SH000300";
  try{
    const r=await fetch(url1,{headers:{"User-Agent":UA,"Referer":"https://danjuanfunds.com"},timeout:15000});
    if(r.ok){ const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm); if(Number.isFinite(v)&&v>0) return {v,tag:"真实",link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")'} }
  }catch{}
  try{
    const r=await fetch(url1b,{headers:{"User-Agent":UA,"Referer":"https://danjuanfunds.com"},timeout:15000});
    if(r.ok){ const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm); if(Number.isFinite(v)&&v>0) return {v,tag:"真实",link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")'} }
  }catch{}
  try{
    const r=await fetch("https://danjuanfunds.com/index-detail/SH000300",{headers:{"User-Agent":UA},timeout:15000});
    if(r.ok){ const html=await r.text(); const m=html.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i); if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return {v,tag:"真实",link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")'} } }
  }catch{}
  // 兜底
  const vOv = OV("PE_OVERRIDE");
  return { v: vOv??"", tag: vOv?"兜底":"", link:"—" };
}

// SPX：蛋卷 SP500
async function pe_spx(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/SP500";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const html=await r.text();
      const mJson=html.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){ const v=Number(mJson[1]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Danjuan")` }; }
      const mTxt=strip(html).match(/(PE|市盈率)[^0-9]{0,6}([\d.]+)/i);
      if(mTxt){ const v=Number(mTxt[2]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Danjuan")` }; }
    }
  }catch{}
  const ov=OV("PE_OVERRIDE_SPX"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}

// NDX：蛋卷 NDX
async function pe_ndx(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/NDX";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const html=await r.text();
      const mJson=html.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){ const v=Number(mJson[1]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Danjuan")` }; }
      const mTxt=strip(html).match(/(PE|市盈率)[^0-9]{0,6}([\d.]+)/i);
      if(mTxt){ const v=Number(mTxt[2]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Danjuan")` }; }
    }
  }catch{}
  const ov=OV("PE_OVERRIDE_NDX"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}

// DAX：Yahoo Finance
async function pe_dax(){
  const url="https://finance.yahoo.com/quote/DAX/";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA, "Accept-Language":"en-US,en" }, timeout:15000 });
    if(r.ok){
      const html=await r.text();
      const mJson=html.match(/"trailingPE"\s*:\s*{\s*"raw"\s*:\s*([\d.]+)/i);
      if(mJson){ const v=Number(mJson[1]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Yahoo Finance")` }; }
      const mTxt=strip(html).match(/PE\s*Ratio\s*\(TTM\)[^0-9]*([\d.]+)/i);
      if(mTxt){ const v=Number(mTxt[1]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Yahoo Finance")` }; }
    }
  }catch{}
  const ov=OV("PE_OVERRIDE_DAX"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}

// N225：Nikkei PER（archives）
async function pe_n225(){
  const url="https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:20000 });
    if(r.ok){
      const html=await r.text(); const text=strip(html);
      const m=text.match(/PER[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Nikkei PER archives")` }; }
    }
  }catch{}
  const ov=OV("PE_OVERRIDE_N225"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}

// ASX200：S&P DJI（大量JS → 先走兜底；若 USE_PLAYWRIGHT=1 再尝试）
async function pe_asx200(){
  const url="https://hk.finance.yahoo.com/quote/STW.AX/";
  if(USE_PLAYWRIGHT){
    try{
      const { chromium } = await import("playwright");
      const b=await chromium.launch({ headless:true }); const p=await b.newPage();
      p.setDefaultNavigationTimeout(15000); p.setDefaultTimeout(12000);
      await p.goto(url,{ waitUntil:"domcontentloaded" });
      const text=await p.locator("body").innerText();
      await b.close();
      const m=text.match(/P\/E\s*(?:Ratio)?[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","S&P DJI")` }; }
    }catch{}
  }
  const ov=OV("PE_OVERRIDE_ASX200"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}

// NIFTY50：Trendlyne（大量JS → Playwright优先；失败→兜底）
async function pe_nifty50(){
  const url="https://trendlyne.com/equity/PE/NIFTY/1887/nifty-50-price-to-earning-ratios/";
  if(USE_PLAYWRIGHT){
    try{
      const { chromium } = await import("playwright");
      const b=await chromium.launch({ headless:true }); const p=await b.newPage();
      p.setDefaultNavigationTimeout(15000); p.setDefaultTimeout(12000);
      await p.goto(url,{ waitUntil:"domcontentloaded" });
      const text=await p.locator("body").innerText();
      await b.close();
      const m = text.match(/P\/?E[^0-9]*([\d.]+)/i) || text.match(/TTM[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Trendlyne")` }; }
    }catch{}
  }
  const ov=OV("PE_OVERRIDE_NIFTY50"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}

// CSIH30533：蛋卷估值页（r_f 使用“美国10Y”）
async function pe_csiH30533(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const html=await r.text();
      const mJson=html.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){ const v=Number(mJson[1]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Danjuan")` }; }
      const mTxt=strip(html).match(/(PE|市盈率)[^0-9]{0,6}([\d.]+)/i);
      if(mTxt){ const v=Number(mTxt[2]); if(Number.isFinite(v)&&v>0) return { v,tag:"真实",link:`=HYPERLINK("${url}","Danjuan")` }; }
    }
  }catch{}
  const ov=OV("PE_OVERRIDE_CSIH30533"); return { v: ov??"", tag: ov?"兜底":"", link: ov?"—":"—" };
}

// 顶部总览（6 指数；各自国家10Y）
const SUMMARY_INDEXES = [
  { title:"标普500",     country:"USA",       peFn: pe_spx },
  { title:"纳斯达克100", country:"USA",       peFn: pe_ndx },
  { title:"德国DAX",     country:"Germany",   peFn: pe_dax },
  { title:"日经225",     country:"Japan",     peFn: pe_n225 },
  { title:"澳洲ASX200",  country:"Australia", peFn: pe_asx200 },
  { title:"印度Nifty50", country:"India",     peFn: pe_nifty50 },
];

// 详表顺序：HS300 → 六海外 → CSIH30533（其 r_f 用 USA）
const DETAIL_INDEXES = [
  { title:"标普500",     country:"USA",       pe: pe_spx,     home:'=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/SP500","Danjuan")', peDesc:"蛋卷估值页（SP500）" },
  { title:"纳斯达克100", country:"USA",       pe: pe_ndx,     home:'=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/NDX","Danjuan")',   peDesc:"蛋卷估值页（NDX）" },
  { title:"德国DAX",     country:"Germany",   pe: pe_dax,     home:'=HYPERLINK("https://finance.yahoo.com/quote/DAX/","Yahoo Finance")',                 peDesc:"Yahoo Finance（DAX）" },
  { title:"日经225",     country:"Japan",     pe: pe_n225,    home:'=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per","Nikkei")', peDesc:"Nikkei 官方 PER" },
  { title:"澳洲ASX200",  country:"Australia", pe: pe_asx200,  home:'=HYPERLINK("https://www.spglobal.com/spdji/en/indices/equity/sp-asx-200/#overview","S&P DJI")', peDesc:"S&P DJI 指数页" },
  { title:"印度Nifty50", country:"India",     pe: pe_nifty50, home:'=HYPERLINK("https://trendlyne.com/equity/PE/NIFTY/1887/nifty-50-price-to-earning-ratios/","Trendlyne")', peDesc:"Trendlyne（Nifty50 PE）" },

  // 中概互联网（特指使用美国10Y）
  { title:"中概互联网（CSIH30533）", country:"USA", pe: pe_csiH30533,
    home:'=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533","Danjuan")', peDesc:"蛋卷估值页（CSIH30533）" },
];

// 顶部总览
async function writeGlobalSummary(erpMap){
  const { sheetTitle, sheetId } = await ensureToday();
  const rows = [["指数名称","当前PE","估值水平","备注"]];

  for(const it of SUMMARY_INDEXES){
    const peRes = await it.peFn(); const pe = peRes.v;
    const { v:rfV } = await rf(it.country);
    const erpStar = erpMap[it.country];

    let level="—", note="";
    if(Number.isFinite(Number(pe)) && Number.isFinite(rfV) && Number.isFinite(erpStar)){
      const ep = 1/Number(pe); const implied = ep - rfV;
      if(implied >= erpStar + DELTA) level="🟢 低估";
      else if(implied <= erpStar - DELTA) level="🔴 高估";
      else level="🟡 合理";
    }else{
      if(!Number.isFinite(Number(pe))) note="（PE待接入/兜底）";
      else if(!Number.isFinite(rfV))   note="（r_f缺失）";
      else if(!Number.isFinite(erpStar)) note="（ERP*缺失）";
    }
    rows.push([it.title, Number.isFinite(Number(pe))? Number(pe):"", level, note]);
  }

  await write(`'${sheetTitle}'!A1:D${rows.length}`, rows);
  await batch([
    { repeatCell:{ range:{ sheetId, startRowIndex:0, endRowIndex:1, startColumnIndex:0, endColumnIndex:4 },
      cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
      fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:0, endIndex:1 }, properties:{ pixelSize:180 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:1, endIndex:2 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:2, endIndex:3 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:3, endIndex:4 }, properties:{ pixelSize:220 }, fields:"pixelSize" } },
  ]);

  return rows.length + 1; // 下一块起始行（空一行）
}

// HS300 详表
async function writeHS300Block(startRow){
  const { sheetTitle, sheetId } = await ensureToday();
  const peRes = await pe_hs300(); const pe = peRes.v; const peTag = peRes.tag || (pe!=""?"真实":"");

  // 中国10Y
  const { v:rfCN, tag:rfTag, link:rfLink } = await rf("China");

  const ep = Number.isFinite(pe) ? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rfCN)) ? (ep - rfCN) : null;
  const peLimit = Number.isFinite(rfCN) ? Number((1/(rfCN + ERP_TARGET_CN)).toFixed(2)) : null;
  let status="需手动更新";
  if(implied!=null){
    if(implied >= ERP_TARGET_CN + DELTA) status="🟢 买点（低估）";
    else if(implied <= ERP_TARGET_CN - DELTA) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数","沪深300","真实","宽基指数估值分块", '=HYPERLINK("https://www.csindex.com.cn/zh-CN/indices/index-detail/000300","中证指数有限公司")'],
    ["P/E（TTM）", pe ?? "", peTag, "蛋卷 index-detail（JSON→HTML）", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", pe!=""?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rfCN ?? "", rfTag, "Investing.com 中国10Y", rfLink],
    ["隐含ERP = E/P − r_f", implied ?? "", implied!=null?"真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", ERP_TARGET_CN, "真实", "建议参考达摩达兰", '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", peLimit!=null?"真实":"兜底", "直观对照","—"],
    ["判定", status, implied!=null?"真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];
  const endRow = startRow + rows.length - 1;

  await write(`'${sheetTitle}'!A${startRow}:E${endRow}`, rows);
  await batch([
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow-1, endRowIndex:startRow, startColumnIndex:0, endColumnIndex:5 },
      cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
      fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } },
    // 列宽
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:0, endIndex:1 }, properties:{ pixelSize:140 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:1, endIndex:2 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:2, endIndex:3 }, properties:{ pixelSize:80  }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:3, endIndex:4 }, properties:{ pixelSize:420 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:4, endIndex:5 }, properties:{ pixelSize:260 }, fields:"pixelSize" } },
    // B列数值格式
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+1, endRowIndex:startRow+2, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+2, endRowIndex:startRow+7, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } }, fields:"userEnteredFormat.numberFormat" } },
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+7, endRowIndex:startRow+8, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
    // “数据”列居中
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow, startColumnIndex:2, endColumnIndex:3 }, cell:{ userEnteredFormat:{ horizontalAlignment:"CENTER" } }, fields:"userEnteredFormat.horizontalAlignment" } }
  ]);

  return endRow + 2;
}

// 海外分块（与 HS300 同款）
async function writeDetailBlock(startRow, cfg, erpMap){
  const { sheetTitle, sheetId } = await ensureToday();
  const peRes = await cfg.pe(); const pe = peRes.v; const peTag = peRes.tag || (pe!=""?"真实":"");
  const rfRes = await rf(cfg.country); const rfV = rfRes.v; const rfTag = rfRes.tag || (rfV!=null?"真实":"");
  const erpStar = erpMap[cfg.country];

  const ep = Number.isFinite(pe)? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rfV)) ? (ep - rfV) : null;

  let status="需手动更新";
  if (implied!=null && Number.isFinite(erpStar)) {
    if (implied >= erpStar + DELTA) status="🟢 买点（低估）";
    else if (implied <= erpStar - DELTA) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数", cfg.title, "真实", "宽基指数估值分块", cfg.home || "—"],
    ["P/E（TTM）", pe ?? "", peTag, cfg.peDesc || "—", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", pe!=""?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rfV ?? "", rfTag, "Investing.com 10Y", rfRes.link || "—"],
    ["隐含ERP = E/P − r_f", implied ?? "", implied!=null?"真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", Number.isFinite(erpStar)? erpStar:"", Number.isFinite(erpStar)?"真实":"兜底", "达摩达兰国家表", '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", (Number.isFinite(rfV)&&Number.isFinite(erpStar))? Number((1/(rfV+erpStar)).toFixed(2)):"", (Number.isFinite(rfV)&&Number.isFinite(erpStar))?"真实":"兜底", "直观对照","—"],
    ["判定", status, (implied!=null&&Number.isFinite(erpStar))? "真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];
  const endRow = startRow + rows.length - 1;

  await write(`'${sheetTitle}'!A${startRow}:E${endRow}`, rows);
  await batch([
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow-1, endRowIndex:startRow, startColumnIndex:0, endColumnIndex:5 },
      cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
      fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:0, endIndex:1 }, properties:{ pixelSize:140 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:1, endIndex:2 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:2, endIndex:3 }, properties:{ pixelSize:80  }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:3, endIndex:4 }, properties:{ pixelSize:420 }, fields:"pixelSize" } },
    { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:4, endIndex:5 }, properties:{ pixelSize:260 }, fields:"pixelSize" } },
    // B列格式：P/E 两位小数；E/P~δ 百分比；P/E上限 两位小数
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+1, endRowIndex:startRow+2, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+2, endRowIndex:startRow+7, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } }, fields:"userEnteredFormat.numberFormat" } },
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow+7, endRowIndex:startRow+8, startColumnIndex:1, endColumnIndex:2 }, cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" } },
    // “数据”列居中
    { repeatCell:{ range:{ sheetId, startRowIndex:startRow, startColumnIndex:2, endColumnIndex:3 }, cell:{ userEnteredFormat:{ horizontalAlignment:"CENTER" } }, fields:"userEnteredFormat.horizontalAlignment" } }
  ]);

  return endRow + 2;
}

// 主流程：顶部总览 → HS300 → 6 海外 → CSIH30533
async function main(){
  const { sheetTitle } = await ensureToday();
  const erpMap = await fetchERPMap();

  // 1) 顶部全市场指数
  const nextRow = await (async ()=>{
    const { sheetId } = await ensureToday();
    const rows = [["指数名称","当前PE","估值水平","备注"]];
    for(const it of SUMMARY_INDEXES){
      const peRes = await it.peFn(); const pe = peRes.v;
      const { v:rfV } = await rf(it.country);
      const erpStar = erpMap[it.country];
      let level="—", note="";
      if(Number.isFinite(Number(pe)) && Number.isFinite(rfV) && Number.isFinite(erpStar)){
        const implied = 1/Number(pe) - rfV;
        level = implied >= erpStar + DELTA ? "🟢 低估" : implied <= erpStar - DELTA ? "🔴 高估" : "🟡 合理";
      }else{
        if(!Number.isFinite(Number(pe))) note="（PE待接入/兜底）";
        else if(!Number.isFinite(rfV)) note="（r_f缺失）";
        else if(!Number.isFinite(erpStar)) note="（ERP*缺失）";
      }
      rows.push([it.title, Number.isFinite(Number(pe))? Number(pe) : "", level, note]);
    }
    await write(`'${sheetTitle}'!A1:D${rows.length}`, rows);
    await batch([
      { repeatCell:{ range:{ sheetId, startRowIndex:0, endRowIndex:1, startColumnIndex:0, endColumnIndex:4 },
        cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
        fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } },
      { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:0, endIndex:1 }, properties:{ pixelSize:180 }, fields:"pixelSize" } },
      { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:1, endIndex:2 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
      { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:2, endIndex:3 }, properties:{ pixelSize:120 }, fields:"pixelSize" } },
      { updateDimensionProperties:{ range:{ sheetId, dimension:"COLUMNS", startIndex:3, endIndex:4 }, properties:{ pixelSize:220 }, fields:"pixelSize" } },
    ]);
    return rows.length + 1; // 下一块起始行
  })();

  // 2) HS300 详表
  let row = await writeHS300Block(nextRow);

  // 3) 六个海外 + 4) CSIH30533
  for(const cfg of DETAIL_INDEXES){
    row = await writeDetailBlock(row, cfg, erpMap);
  }

  console.log("[DONE]", sheetTitle);
}

// 执行
main().catch(e => { console.error(e); process.exit(1); });
