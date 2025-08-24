// HS300 + S&P500 —— 仅两块详表，P/E 专抓估值页顶部红圈数字（Playwright）；无汇总表
import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PW = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";
const TZ = process.env.TZ || "Asia/Shanghai";

const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: TZ }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr = (v,d)=>{ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; };
const strip = (h)=>h.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");

// 判定参数
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);  // HS300
const DELTA         = numOr(process.env.DELTA,      0.005);

// 兜底（小数）
const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);  // 中国10Y兜底
const RF_US = numOr(process.env.RF_US,       0.0425);  // 美国10Y兜底
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
      spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title } } }]}
    });
    sh={ properties:add.data.replies[0].addSheet.properties };
  }
  return { sheetTitle:title, sheetId:sh.properties.sheetId };
}
async function write(range, rows){
  await sheets.spreadsheets.values.update({
    spreadsheetId:SPREADSHEET_ID, range, valueInputOption:"USER_ENTERED",
    requestBody:{ values: rows }
  });
}

// ---------------- r_f ----------------
async function rfCN(){
  // 有知有行文本抓取（适度容错）
  try{
    const r=await fetch("https://youzhiyouxing.cn/data",{ headers:{ "User-Agent":"Mozilla/5.0" }, timeout:15000 });
    if(r.ok){
      const html=await r.text();
      let m=html.match(/10年期国债到期收益率[^%]{0,200}?(\d+(?:\.\d+)?)\s*%/);
      if(!m){
        const all=[...html.matchAll(/(\d+(?:\.\d+)?)\s*%/g)].map(x=>Number(x[1])).filter(Number.isFinite);
        if(all.length) m=[null,Math.max(...all).toString()];
      }
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实",
        link:'=HYPERLINK("https://youzhiyouxing.cn/data","Youzhiyouxing")' }; }
    }
  }catch{}
  return { v:RF_CN, tag:"兜底", link:"—" };
}
async function rfUS(){
  const urls=["https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield",
              "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"];
  for(const url of urls){
    try{
      const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:15000 });
      if(!r.ok) continue;
      const html=await r.text(); const m=html.match(/(\d+(?:\.\d+)?)\s*%/);
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实",
        link:`=HYPERLINK("${url}","US 10Y (Investing)")` }; }
    }catch{}
  }
  return { v:RF_US, tag:"兜底", link:"—" };
}

// ---------------- ERP*（US） ----------------
async function erpUS(){
  try{
    const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:20000 });
    if(!r.ok) throw 0;
    const html=await r.text();
    const row=html.split(/<\/tr>/i).find(tr=>/United\s+States/i.test(tr)||/USA/i.test(tr))||"";
    const m=row.replace(/<[^>]+>/g," ").match(/(\d+(?:\.\d+)?)\s*%/);
    if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实",
      link:`=HYPERLINK("${url}","Damodaran(US)")` }; }
  }catch{}
  // 兜底：按你要求 4.33%
  return { v:0.0433, tag:"兜底",
    link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' };
}

// ---------------- Danjuan 顶部 “PE xx.xx” ----------------
async function readTopPEWithPW(url){
  const { chromium } = await import("playwright");
  const br = await chromium.launch({ headless:true });
  const pg = await br.newPage();
  pg.setDefaultNavigationTimeout(20000); pg.setDefaultTimeout(15000);

  // 直接打开估值页；等待页面主要文本就绪
  await pg.goto(url, { waitUntil:"domcontentloaded" });

  // 1) 先看全页可见文本
  const bodyText = await pg.locator("body").innerText();
  let m = bodyText.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
  if(m){ const v=Number(m[1]); await br.close(); if(Number.isFinite(v)&&v>0&&v<1000) return v; }

  // 2) 在 DOM 节点逐项查找
  const v2 = await pg.evaluate(() => {
    const re = /PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i;
    for (const el of Array.from(document.querySelectorAll("body *"))) {
      const t = (el.textContent || "").trim();
      const m = t.match(re);
      if (m) return parseFloat(m[1]);
    }
    return null;
  });
  await br.close();
  return v2;
}

async function readTopPEFallback(url){
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const text=strip(h);
      const mTop=text.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
      if(mTop){ const v=Number(mTop[1]); if(Number.isFinite(v)&&v>0&&v<1000) return v; }
      const mJson=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){ const v=Number(mJson[1]); if(Number.isFinite(v)&&v>0&&v<1000) return v; }
    }
  }catch{}
  return null;
}

async function peFromDanjuan(url, override){
  // Playwright 优先抓红圈
  if (USE_PW) {
    try{ const v=await readTopPEWithPW(url); if(v!=null) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; }catch{}
  }
  // fetch 兜底
  const v2 = await readTopPEFallback(url);
  if (v2!=null) return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` };
  // 覆盖兜底
  if (override!=null) return { v:override, tag:"兜底", link:`=HYPERLINK("${url}","Danjuan")` };
  // 仍失败：写空但给链接（绝不写 0）
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Danjuan")` };
}

async function peHS300(){
  return await peFromDanjuan("https://danjuanfunds.com/dj-valuation-table-detail/SH000300", PE_OVERRIDE_CN);
}
async function peSPX(){
  return await peFromDanjuan("https://danjuanfunds.com/dj-valuation-table-detail/SP500", PE_OVERRIDE_SPX);
}

// ---------------- 写“单块” ----------------
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink){
  const { sheetTitle } = await ensureToday();

  const pe = Number(peRes.v);
  const peTag = peRes.tag || (Number.isFinite(pe) ? "真实" : "兜底");
  const rf = Number.isFinite(rfRes.v) ? rfRes.v : null;
  const rfTag = rfRes.tag || (rf!=null ? "真实" : "兜底");

  const ep = Number.isFinite(pe) ? 1/pe : null;
  const target = (label==="沪深300") ? ERP_TARGET_CN : erpStar;
  const implied = (ep!=null && Number.isFinite(rf)) ? (ep - rf) : null;
  const peLimit = (Number.isFinite(rf) && Number.isFinite(target)) ? Number((1/(rf+target)).toFixed(2)) : null;

  let status = "需手动更新";
  if (implied!=null && Number.isFinite(target)) {
    if (implied >= target + 0.005) status="🟢 买点（低估）";
    else if (implied <= target - 0.005) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数", label, "真实", "宽基指数估值分块", peRes.link || "—"],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peTag, "估值来源", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rfTag, (label==="沪深300"?"有知有行 10Y":"Investing.com 10Y"), rfRes.link || "—"],
    ["隐含ERP = E/P − r_f", implied ?? "", (implied!=null)?"真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", (label==="沪深300"? ERP_TARGET_CN : (Number.isFinite(target)?target:"")), (label==="沪深300"?"真实":(Number.isFinite(target)?"真实":"兜底")),
      (label==="沪深300"?"建议参考达摩达兰":"达摩达兰 United States"), erpLink || '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", 0.005, "真实", "减少频繁切换","—"],
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
  const pe_hs = await peHS300();
  const rf_cn = await rfCN();
  row = await writeBlock(row,"沪深300",pe_hs,rf_cn,null,null,null);  // ERP 取 ERP_TARGET_CN

  // SPX（美国10Y + ERP(US)）
  const pe_spx = await peSPX();
  const rf_us  = await rfUS();
  const erp_us = await erpUS();   // 失败兜底 4.33%
  row = await writeBlock(row,"标普500",pe_spx,rf_us,erp_us.v,erp_us.tag,erp_us.link);

  console.log("[DONE]", todayStr());
})();
