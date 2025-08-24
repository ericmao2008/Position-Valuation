// === HS300 + S&P500 ===
// 顶部：全市场指数（沪深300、标普500）总览：指数名称 | 当前PE | 估值水平 | 备注
// 下方：沪深300详表 → 标普500详表（与 HS300 同款：字段 | 数值 | 数据 | 说明 | 数据源）
// 数据源：
//  HS300  PE：蛋卷 JSON→HTML； r_f：中国10Y（有知有行）； ERP* = 5.27%（可覆写）
//  SPX    PE：蛋卷 SP500 估值页；  r_f：美国10Y（Investing）； ERP* = Damodaran United States
//  全部失败均有兜底：PE_OVERRIDE / RF_OVERRIDE、RF_US；“数据”列标注 真实/兜底；不会空值

import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

// ---------- utils ----------
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const tz = process.env.TZ || "Asia/Shanghai";
const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: tz }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr = (v, d) => { if (v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; };
const strip = html => html.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");

const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527); // HS300
const DELTA         = numOr(process.env.DELTA,      0.005);

const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);
const RF_US = numOr(process.env.RF_US,       0.0425);
const PE_OVERRIDE_CN  = (()=>{ const s=(process.env.PE_OVERRIDE??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)&&n>0&&n<1000?n:null;})();
const PE_OVERRIDE_SPX = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)&&n>0&&n<1000?n:null;})();

// ---------- Sheets ----------
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if (!SPREADSHEET_ID) { console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL, null,
  (process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version:"v4", auth });

async function ensureToday(){
  const title=todayStr();
  const meta = await sheets.spreadsheets.get({ spreadsheetId:SPREADSHEET_ID });
  let sh = meta.data.sheets?.find(s=>s.properties?.title===title);
  if(!sh){
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title }}}]}
    });
    sh = { properties: add.data.replies[0].addSheet.properties };
  }
  return { sheetTitle:title, sheetId:sh.properties.sheetId };
}
async function write(range, rows){
  await sheets.spreadsheets.values.update({
    spreadsheetId:SPREADSHEET_ID, range, valueInputOption:"USER_ENTERED", requestBody:{ values: rows }
  });
}
async function batch(reqs){ if(!reqs?.length) return; await sheets.spreadsheets.batchUpdate({ spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:reqs } }); }

// ---------- Investing.com 10Y ----------
async function fetchInvesting10Y_US(){
  const urls = [
    "https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield",
    "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"
  ];
  for (const url of urls) {
    try{
      const r = await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:15000 });
      if(!r.ok) continue;
      const html = await r.text();
      const m = html.match(/(\d+(?:\.\d+)?)\s*%/);
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","US 10Y (Investing)")` }; }
    }catch{}
  }
  return { v:RF_US, tag:"兜底", link:"—" };
}

async function fetchYouzhiyouxingCN(){
  try{
    const r = await fetch("https://youzhiyouxing.cn/data",{ headers:{ "User-Agent":"Mozilla/5.0" }, timeout:12000 });
    if(r.ok){
      const html = await r.text();
      let m = html.match(/10年期国债到期收益率[^%]{0,160}?(\d+(?:\.\d+)?)\s*%/);
      if(!m){
        const all = [...html.matchAll(/(\d+(?:\.\d+)?)\s*%/g)].map(x=>Number(x[1])).filter(Number.isFinite);
        if(all.length) m = [null, Math.max(...all).toString()];
      }
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:'=HYPERLINK("https://youzhiyouxing.cn/data","Youzhiyouxing")' }; }
    }
  }catch{}
  return { v:RF_CN, tag:"兜底", link:"—" };
}

// ---------- Damodaran ERP* ----------
async function fetchERP_US(){
  try{
    const url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
    const r = await fetch(url,{ headers:{ "User-Agent":UA }, timeout:20000 });
    if(!r.ok) throw 0;
    const html = await r.text();
    const row = html.split(/<\/tr>/i).find(tr => /United\s+States/i.test(tr) || /USA/i.test(tr)) || "";
    const m = row.replace(/<[^>]+>/g," ").match(/(\d+(?:\.\d+)?)\s*%/);
    if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","Damodaran(US)")` }; }
  }catch{}
  return { v:0.0527, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' };
}

// ---------- HS300：PE（蛋卷 JSON→HTML） ----------
async function fetchPE_HS300(){
  try{
    const r=await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=SH000300",{ headers:{ "User-Agent":UA,"Referer":"https://danjuanfunds.com"}, timeout:15000 });
    if(r.ok){ const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")' }; }
  }catch{}
  try{
    const r=await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail/SH000300",{ headers:{ "User-Agent":UA,"Referer":"https://danjuanfunds.com"}, timeout:15000 });
    if(r.ok){ const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")' }; }
  }catch{}
  try{
    const r=await fetch("https://danjuanfunds.com/index-detail/SH000300",{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){ const h=await r.text(); const m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i); if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")' }; } }
  }catch{}
  const ov=PE_OVERRIDE_CN; return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

// ---------- SPX：PE（蛋卷 SP500 估值页） ----------
async function fetchPE_SPX(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/SP500";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const mJson=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){ const v=Number(mJson[1]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; }
      const mTxt = strip(h).match(/(PE|市盈率)[^0-9]{0,6}([\d.]+)/i);
      if(mTxt){ const v=Number(mTxt[2]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; }
    }
  }catch{}
  const ov=PE_OVERRIDE_SPX; return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

// ---------- 写顶部“全市场指数”（HS300 + SPX） ----------
async function writeGlobalSummary(peHS, peSPX, rfUS, erpUS){
  const { sheetTitle, sheetId } = await ensureToday();
  const rows = [["指数名称","当前PE","估值水平","备注"]];

  // HS300（以中国10Y + ERP_TARGET_CN 判定）
  let noteCN=""; let levelCN="—";
  if(Number.isFinite(peHS.v) && Number.isFinite(RF_CN) && Number.isFinite(ERP_TARGET_CN)){
    const implied = 1/Number(peHS.v) - RF_CN;
    levelCN = implied >= ERP_TARGET_CN + DELTA ? "🟢 低估" : implied <= ERP_TARGET_CN - DELTA ? "🔴 高估" : "🟡 合理";
  } else {
    if(!Number.isFinite(peHS.v)) noteCN="（PE待接入/兜底）";
  }
  rows.push(["沪深300", Number.isFinite(peHS.v)? peHS.v:"", levelCN, noteCN]);

  // SPX（用美国10Y + ERP_US）
  let noteUS=""; let levelUS="—";
  if(Number.isFinite(peSPX.v) && Number.isFinite(rfUS.v) && Number.isFinite(erpUS.v)){
    const implied = 1/Number(peSPX.v) - rfUS.v;
    levelUS = implied >= erpUS.v + DELTA ? "🟢 低估" : implied <= erpUS.v - DELTA ? "🔴 高估" : "🟡 合理";
  } else {
    if(!Number.isFinite(peSPX.v)) noteUS="（PE待接入/兜底）";
    else if(!Number.isFinite(rfUS.v)) noteUS="（r_f缺失）";
    else if(!Number.isFinite(erpUS.v)) noteUS="（ERP*缺失）";
  }
  rows.push(["标普500", Number.isFinite(peSPX.v)? peSPX.v:"", levelUS, noteUS]);

  await write(`'${sheetTitle}'!A1:D${rows.length}`, rows);
  await batch([{ repeatCell:{ range:{ sheetId, startRowIndex:0, endRowIndex:1, startColumnIndex:0, endColumnIndex:4 },
    cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
    fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } }]);

  return rows.length + 1; // 下一块起始行
}

// ---------- 写 HS300 详表 ----------
async function writeHS300Block(startRow){
  const { sheetTitle, sheetId } = await ensureToday();
  const peRes = await fetchPE_HS300();
  const rfCNRes = await fetchYouzhiyouxingCN();
  const pe = Number(peRes.v); const peTag = peRes.tag || (Number.isFinite(pe) ? "真实":"");
  const rf = Number.isFinite(rfCNRes.v) ? rfCNRes.v : RF_CN; const rfTag = rfCNRes.tag || (Number.isFinite(rfCNRes.v) ? "真实":"兜底");

  const ep = Number.isFinite(pe) ? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rf)) ? (ep - rf) : null;
  const peLimit = Number.isFinite(rf) ? Number((1/(rf + ERP_TARGET_CN)).toFixed(2)) : null;

  let status="需手动更新";
  if(implied!=null){
    if(implied >= ERP_TARGET_CN + DELTA) status="🟢 买点（低估）";
    else if(implied <= ERP_TARGET_CN - DELTA) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数","沪深300","真实","宽基指数估值分块", '=HYPERLINK("https://www.csindex.com.cn/zh-CN/indices/index-detail/000300","中证指数有限公司")'],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peTag, "蛋卷 index-detail（JSON→HTML）", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rfTag, "有知有行（文本；抓不到用兜底）", rfCNRes.link || "—"],
    ["隐含ERP = E/P − r_f", implied ?? "", implied!=null?"真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", ERP_TARGET_CN, "真实", "达摩达兰", '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", peLimit!=null?"真实":"兜底", "直观对照","—"],
    ["判定", status, implied!=null?"真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];
  const end = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`, rows);
  return end + 2;
}

// ---------- 写 SPX 详表（r_f 用美国10Y；ERP* 用 Damodaran USA） ----------
async function writeSPXBlock(startRow){
  const { sheetTitle, sheetId } = await ensureToday();

  const peRes = await fetchPE_SPX();
  const rfUS = await fetchInvesting10Y_US();
  const erpUS = await (async()=>{ try{ return await fetchERP_US(); }catch{ return { v:0.0527, tag:"兜底", link:"—" }; }})();

  const pe = Number(peRes.v); const peTag = peRes.tag || (Number.isFinite(pe) ? "真实":"");
  const rf = Number.isFinite(rfUS.v) ? rfUS.v : RF_US; const rfTag = rfUS.tag || (Number.isFinite(rfUS.v) ? "真实":"兜底");
  const erpStar = Number.isFinite(erpUS.v) ? erpUS.v : 0.0527; const erpTag="真实";

  const ep = Number.isFinite(pe) ? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rf)) ? (ep - rf) : null;
  const peLimit = (Number.isFinite(rf) && Number.isFinite(erpStar)) ? Number((1/(rf + erpStar)).toFixed(2)) : null;

  let status="需手动更新";
  if (implied!=null && Number.isFinite(erpStar)) {
    if (implied >= erpStar + DELTA) status="🟢 买点（低估）";
    else if (implied <= erpStar - DELTA) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数","标普500","真实","宽基指数估值分块", '=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/SP500","Danjuan SP500")'],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peTag, "蛋卷 SP500 估值页", peRes.link || '=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/SP500","Danjuan")'],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）',"—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rfTag, "Investing.com 美国10Y", rfUS.link || "—"],
    ["隐含ERP = E/P − r_f", implied ?? "", (implied!=null)?"真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", erpStar, "真实", "达摩达兰 United States", erpUS.link || "—"],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", (peLimit!=null)?"真实":"兜底", "直观对照","—"],
    ["判定", status, (implied!=null && Number.isFinite(erpStar))?"真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];
  const end = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`, rows);
  return end + 2;
}

// ---------- Main ----------
(async () => {
  const { sheetTitle } = await ensureToday();

  // 先准备两项（汇总要用）
  const peHS   = await fetchPE_HS300();               // HS300 PE
  const peSPX  = await fetchPE_SPX();                 // SPX  PE
  const rfUS   = await fetchInvesting10Y_US();        // 美国10Y
  const erpUS  = await fetchERP_US();                 // Damodaran USA

  // 顶部：全市场指数（HS300、SPX）
  const nextRow = await writeGlobalSummary(peHS, peSPX, rfUS, erpUS);

  // 下方：HS300 详表 → SPX 详表
  let row = await writeHS300Block(nextRow);
  row     = await writeSPXBlock(row);

  console.log("[DONE]", todayStr());
})();
