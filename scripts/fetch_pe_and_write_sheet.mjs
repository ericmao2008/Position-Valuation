// === HS300 + S&P500（按详表依次排列；抓蛋卷页面顶部“PE xx.xx”红圈数字） ===

import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PLAYWRIGHT = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";
const TZ = process.env.TZ || "Asia/Shanghai";

const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: TZ }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr = (v,d)=>{ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; };
const strip = (h)=>h.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");

// 判定参数
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.005);

// 兜底
const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);
const RF_US = numOr(process.env.RF_US,       0.0425);
const PE_OVERRIDE_CN  = (()=>{ const s=(process.env.PE_OVERRIDE??"").trim();      return s?Number(s):null; })();
const PE_OVERRIDE_SPX = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim();  return s?Number(s):null; })();

// Sheets
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if(!SPREADSHEET_ID){ console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL, null,
  (process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version:"v4", auth });

async function ensureToday(){
  const title=todayStr();
  const meta=await sheets.spreadsheets.get({ spreadsheetId:SPREADSHEET_ID });
  let sh=meta.data.sheets?.find(s=>s.properties?.title===title);
  if(!sh){
    const add=await sheets.spreadsheets.batchUpdate({
      spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title }}}] }
    });
    sh={ properties:add.data.replies[0].addSheet.properties };
  }
  return { sheetTitle:title, sheetId:sh.properties.sheetId };
}
async function write(range, rows){
  await sheets.spreadsheets.values.update({
    spreadsheetId:SPREADSHEET_ID, range, valueInputOption:"USER_ENTERED", requestBody:{ values: rows }
  });
}

// ---------------- r_f ----------------
async function fetchRF_CN(){
  try{
    const r=await fetch("https://youzhiyouxing.cn/data",{ headers:{ "User-Agent":"Mozilla/5.0" }, timeout:12000 });
    if(r.ok){
      const html=await r.text();
      let m=html.match(/10年期国债到期收益率[^%]{0,180}?(\d+(?:\.\d+)?)\s*%/);
      if(!m){
        const all=[...html.matchAll(/(\d+(?:\.\d+)?)\s*%/g)].map(x=>Number(x[1])).filter(Number.isFinite);
        if(all.length) m=[null, Math.max(...all).toString()];
      }
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:'=HYPERLINK("https://youzhiyouxing.cn/data","Youzhiyouxing")' }; }
    }
  }catch{}
  return { v:RF_CN, tag:"兜底", link:"—" };
}
async function fetchRF_US(){
  const urls=[
    "https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield",
    "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"
  ];
  for(const url of urls){
    try{
      const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:15000 });
      if(!r.ok) continue;
      const html=await r.text(); const m=html.match(/(\d+(?:\.\d+)?)\s*%/);
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","US 10Y (Investing)")` }; }
    }catch{}
  }
  return { v:RF_US, tag:"兜底", link:"—" };
}

// ---------------- ERP* ----------------
async function fetchERP_US(){
  try{
    const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:20000 });
    if(!r.ok) throw 0;
    const html=await r.text();
    const row=html.split(/<\/tr>/i).find(tr=>/United\s+States/i.test(tr)||/USA/i.test(tr))||"";
    const m=row.replace(/<[^>]+>/g," ").match(/(\d+(?:\.\d+)?)\s*%/);
    if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","Damodaran(US)")` }; }
  }catch{}
  // 兜底按你的要求 4.33%
  return { v:0.0433, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' };
}

// ---------------- Danjuan 顶部 PE（红圈） ----------------
async function scrapeDanjuanPE(url){
  // 先用 Playwright 抓网页正文（更稳）
  if (USE_PLAYWRIGHT) {
    try{
      const { chromium } = await import("playwright");
      const br = await chromium.launch({ headless:true });
      const pg = await br.newPage();
      pg.setDefaultNavigationTimeout(20000); pg.setDefaultTimeout(15000);
      await pg.goto(url, { waitUntil:"domcontentloaded" });
      const text = await pg.locator("body").innerText();
      await br.close();
      const mTop = text.match(/PE[\s\S]{0,60}?(\d{1,3}\.\d{1,2})/i);  // 容忍中间有日期/间隔字符
      if(mTop){ const v=Number(mTop[1]); if(Number.isFinite(v)&&v>0&&v<1000) return v; }
    }catch{}
  }
  // 再用 fetch 抓源码 + 内嵌 JSON 兜底
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const text=strip(h);
      const mTop=text.match(/PE[\s\S]{0,60}?(\d{1,3}\.\d{1,2})/i);
      if(mTop){ const v=Number(mTop[1]); if(Number.isFinite(v)&&v>0&&v<1000) return v; }
      const mJson=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){ const v=Number(mJson[1]); if(Number.isFinite(v)&&v>0&&v<1000) return v; }
    }
  }catch{}
  return null;
}

async function fetchPE_HS300(){
  const prefer="https://danjuanfunds.com/dj-valuation-table-detail/SH000300";
  const v = await scrapeDanjuanPE(prefer);
  if(v!=null) return { v, tag:"真实", link:`=HYPERLINK("${prefer}","Danjuan SH000300")` };
  if(PE_OVERRIDE_CN!=null) return { v:PE_OVERRIDE_CN, tag:"兜底", link:`=HYPERLINK("${prefer}","Danjuan SH000300")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${prefer}","Danjuan SH000300")` };
}
async function fetchPE_SPX(){
  const prefer="https://danjuanfunds.com/dj-valuation-table-detail/SP500";
  const v = await scrapeDanjuanPE(prefer);
  if(v!=null) return { v, tag:"真实", link:`=HYPERLINK("${prefer}","Danjuan SP500")` };
  if(PE_OVERRIDE_SPX!=null) return { v:PE_OVERRIDE_SPX, tag:"兜底", link:`=HYPERLINK("${prefer}","Danjuan SP500")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${prefer}","Danjuan SP500")` };
}

// ---------------- 写“单块” ----------------
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink){
  const { sheetTitle } = await ensureToday();

  const pe = Number(peRes.v); const peTag = peRes.tag || (Number.isFinite(pe)?"真实":"兜底");
  const rf = Number.isFinite(rfRes.v)? rfRes.v : null; const rfTag = rfRes.tag || (Number.isFinite(rfRes.v)?"真实":"兜底");
  const ep = Number.isFinite(pe)? 1/pe : null;

  const target = (label==="沪深300") ? ERP_TARGET_CN : erpStar;
  const implied = (ep!=null && Number.isFinite(rf)) ? (ep - rf) : null;
  const peLimit = (Number.isFinite(rf) && Number.isFinite(target)) ? Number((1/(rf+target)).toFixed(2)) : null;

  let status="需手动更新";
  if(implied!=null && Number.isFinite(target)){
    if(implied >= target + DELTA) status="🟢 买点（低估）";
    else if(implied <= target - DELTA) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数", label, "真实", "宽基指数估值分块", peRes.link || "—"],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peTag, "估值来源", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rfTag, (label==="沪深300"?"有知有行 10Y":"Investing.com 10Y"), rfRes.link || "—"],
    ["隐含ERP = E/P − r_f", implied ?? "", implied!=null?"真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", (label==="沪深300"? ERP_TARGET_CN : (Number.isFinite(target)?target:"")), (label==="沪深300"?"真实":(Number.isFinite(target)?"真实":"兜底")),
      (label==="沪深300"?"建议参考达摩达兰":"达摩达兰 United States"), erpLink || '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", (peLimit!=null)?"真实":"兜底", "直观对照","—"],
    ["判定", status, (implied!=null && Number.isFinite(target))?"真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];

  const end = startRow + rows.length - 1;
  await write(`'${todayStr()}'!A${startRow}:E${end}`, rows);
  return end + 2;
}

// ---------------- Main：依次写“沪深300 ➜ 标普500” ----------------
(async()=>{
  let row=1;

  // HS300（中国10Y）
  const peHS = await fetchPE_HS300();
  const rfCN = await fetchRF_CN();
  row = await writeBlock(row,"沪深300",peHS,rfCN,null,null,null);

  // SPX（美国10Y + ERP(US)）
  const peSPX = await fetchPE_SPX();
  const rfUS  = await fetchRF_US();
  const erpUS = await (async()=>{ try{ return await (async function(){
      const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
      const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:20000 });
      if(!r.ok) throw 0;
      const h=await r.text(); const row=h.split(/<\/tr>/i).find(tr=>/United\s+States/i.test(tr)||/USA/i.test(tr))||"";
      const m=row.replace(/<[^>]+>/g," ").match(/(\d+(?:\.\d+)?)\s*%/);
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","Damodaran(US)")` }; }
      throw 0;
    })(); }catch{ return { v:0.0433, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }})();

  row = await writeBlock(row,"标普500",peSPX,rfUS,erpUS.v,erpUS.tag,erpUS.link);

  console.log("[DONE]", todayStr());
})();
