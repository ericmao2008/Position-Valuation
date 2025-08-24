// HS300 + S&P500 —— 两块详表；Playwright 在页面内同源 fetch /djapi，再退回正文正则；大量 [DEBUG]
import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PW = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";
const DEBUG  = String(process.env.DEBUG_VERBOSE ?? "0") === "1";
const TZ     = process.env.TZ || "Asia/Shanghai";
const dbg = (...a)=>{ if(DEBUG) console.log("[DEBUG]", ...a); };

const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: TZ }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr = (v,d)=>{ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; };
const strip = (h)=>h.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");

// 判定参数
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.005);

// 兜底（小数）
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
  dbg("Sheet write", range, "rows:", rows.length);
  await sheets.spreadsheets.values.update({
    spreadsheetId:SPREADSHEET_ID, range, valueInputOption:"USER_ENTERED",
    requestBody:{ values: rows }
  });
}

// ---------------- r_f ----------------
async function rfCN(){
  dbg("rfCN: start");
  try{
    const r=await fetch("https://youzhiyouxing.cn/data",{ headers:{ "User-Agent":"Mozilla/5.0" }, timeout:15000 });
    dbg("rfCN: status", r.status);
    if(r.ok){
      const html=await r.text(); dbg("rfCN: html len", html.length);
      let m=html.match(/10年期国债到期收益率[^%]{0,200}?(\d+(?:\.\d+)?)\s*%/);
      if(!m){
        const all=[...html.matchAll(/(\d+(?:\.\d+)?)\s*%/g)].map(x=>Number(x[1])).filter(Number.isFinite);
        dbg("rfCN: pct list", all.slice(0,10)); if(all.length) m=[null, Math.max(...all).toString()];
      }
      if(m){ const v=Number(m[1])/100; dbg("rfCN: parsed", v); if(Number.isFinite(v)&&v>0&&v<1)
        return { v, tag:"真实", link:'=HYPERLINK("https://youzhiyouxing.cn/data","Youzhiyouxing")' }; }
    }
  }catch(e){ dbg("rfCN error", e.message); }
  dbg("rfCN: fallback", RF_CN);
  return { v:RF_CN, tag:"兜底", link:"—" };
}
async function rfUS(){
  dbg("rfUS: start");
  const urls=["https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield",
              "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"];
  for(const url of urls){
    try{
      const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:15000 });
      dbg("rfUS try", url, "status", r.status);
      if(!r.ok) continue;
      const h=await r.text(); dbg("rfUS html len", h.length);
      // 尝试从主要价格区块抓，匹配不到再退回全页正则；限制 0<值<20
      let v=null;
      const m1=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,2})</i);
      if(m1) v=Number(m1[1]);
      if(!Number.isFinite(v)) {
        const text=strip(h);
        const m2=text.match(/(Yield|收益率)[^%]{0,40}?(\d{1,2}\.\d{1,2})\s*%/i) || text.match(/(\d{1,2}\.\d{1,2})\s*%/);
        if(m2) v=Number(m2[2]||m2[1]);
      }
      dbg("rfUS parsed pct", v);
      if(Number.isFinite(v)&&v>0&&v<20) return { v:v/100, tag:"真实", link:`=HYPERLINK("${url}","US 10Y (Investing)")` };
    }catch(e){ dbg("rfUS error", url, e.message); }
  }
  dbg("rfUS: fallback", RF_US);
  return { v:RF_US, tag:"兜底", link:"—" };
}

// ---------------- ERP*（US） ----------------
async function erpUS(){
  dbg("erpUS: start");
  try{
    const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:20000 });
    dbg("erpUS status", r.status);
    if(!r.ok) throw 0;
    const h=await r.text(); dbg("erpUS html len", h.length);
    const row=h.split(/<\/tr>/i).find(tr=>/United\s+States/i.test(tr)||/USA/i.test(tr))||"";
    const text=row.replace(/<[^>]+>/g," ");
    const pcts=[...text.matchAll(/(\d{1,2}\.\d{1,2})\s*%/g)].map(m=>Number(m[1]));
    dbg("erpUS row pcts", pcts);
    const lastNonZero = pcts.reverse().find(x=>x>0);
    if(Number.isFinite(lastNonZero)) return { v:lastNonZero/100, tag:"真实", link:`=HYPERLINK("${url}","Damodaran(US)")` };
  }catch(e){ dbg("erpUS error", e.message); }
  dbg("erpUS: fallback 0.0433");
  return { v:0.0433, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' };
}

// ---------------- Danjuan：在页面内同源 fetch /djapi ----------------
async function readPEFromDjapiInPage(page, indexCode){
  return await page.evaluate(async (code)=>{
    try {
      const u = `/djapi/index_evaluation/detail?index_code=${code}`;
      const r = await fetch(u, { credentials: "include" });
      if (!r.ok) return null;
      const j = await r.json();
      return Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm) || null;
    } catch { return null; }
  }, indexCode);
}

async function readTopPEWithPW(url, indexCode){
  const { chromium } = await import("playwright");
  const br = await chromium.launch({ headless:true });
  const pg = await br.newPage();
  pg.setDefaultNavigationTimeout(25000); pg.setDefaultTimeout(20000);

  await pg.goto(url, { waitUntil:"networkidle" });
  dbg("PW goto ok", url);

  // 1) 页面内同源 djapi
  let v = await readPEFromDjapiInPage(pg, indexCode);
  dbg("PW djapi value", v);

  // 2) djapi 为空时尝试正文
  if(!Number.isFinite(v)){
    const text = await pg.locator("body").innerText();
    dbg("PW body len", text.length);
    const m = text.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
    v = m ? Number(m[1]) : null;
    dbg("PW body regex", v);
  }

  // 3) DOM 枚举兜底
  if(!Number.isFinite(v)){
    v = await pg.evaluate(()=>{
      const re = /PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i;
      for(const el of Array.from(document.querySelectorAll("body *"))){
        const t = (el.textContent || "").trim();
        const m = t.match(re);
        if(m) return parseFloat(m[1]);
      }
      return null;
    });
    dbg("PW DOM scan", v);
  }

  await br.close();
  return Number.isFinite(v)&&v>0&&v<1000 ? v : null;
}

async function readTopPEFallback(url){
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    dbg("HTTP fallback status", r.status);
    if(r.ok){
      const h=await r.text();
      const text=strip(h);
      const mTop=text.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
      if(mTop){ const v=Number(mTop[1]); dbg("HTTP regex", v); if(Number.isFinite(v)&&v>0&&v<1000) return v; }
      const mJson=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){ const v=Number(mJson[1]); dbg("HTTP json pe_ttm", v); if(Number.isFinite(v)&&v>0&&v<1000) return v; }
    }
  }catch(e){ dbg("HTTP fallback err", e.message); }
  return null;
}

async function peFromDanjuan(url, indexCode, override){
  dbg("peFromDanjuan", url, "USE_PW=", USE_PW);
  if (USE_PW) {
    try{
      const v = await readTopPEWithPW(url, indexCode);
      dbg("PW result", v);
      if (v!=null) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` };
    }catch(e){ dbg("PW err", e.message); }
  }
  const v2 = await readTopPEFallback(url);
  dbg("HTTP result", v2);
  if (v2!=null) return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` };
  if (override!=null) { dbg("use override", override); return { v:override, tag:"兜底", link:`=HYPERLINK("${url}","Danjuan")` }; }
  dbg("no value, return empty");
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Danjuan")` };
}

async function peHS300(){ return await peFromDanjuan("https://danjuanfunds.com/dj-valuation-table-detail/SH000300", "SH000300", PE_OVERRIDE_CN); }
async function peSPX(){   return await peFromDanjuan("https://danjuanfunds.com/dj-valuation-table-detail/SP500",    "SP500",    PE_OVERRIDE_SPX); }

// ---------------- 写“单块” ----------------
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink){
  const { sheetTitle } = await ensureToday();

  const pe = Number(peRes.v);
  const rf = Number.isFinite(rfRes.v) ? rfRes.v : null;
  const target = (label==="沪深300") ? ERP_TARGET_CN : erpStar;

  const ep = Number.isFinite(pe) ? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rf)) ? (ep - rf) : null;
  const peLimit = (Number.isFinite(rf) && Number.isFinite(target)) ? Number((1/(rf+target)).toFixed(2)) : null;

  dbg(`${label} values`, { pe, rf, target, ep, implied, peLimit, peTag:peRes.tag, rfTag:rfRes.tag });

  let status="需手动更新";
  if(implied!=null && Number.isFinite(target)){
    if(implied >= target + 0.005) status="🟢 买点（低估）";
    else if(implied <= target - 0.005) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数", label, "真实", "宽基指数估值分块", peRes.link || "—"],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peRes.tag || (Number.isFinite(pe)?"真实":"兜底"), "估值来源", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rfRes.tag || (rf!=null?"真实":"兜底"), (label==="沪深300"?"有知有行 10Y":"Investing.com 10Y"), rfRes.link || "—"],
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

// ---------------- Main ----------------
(async()=>{
  console.log("[INFO] Run start", todayStr(), "USE_PLAYWRIGHT=", USE_PW, "TZ=", TZ);

  let row=1;

  // HS300（中国10Y）
  const pe_hs = await peHS300();
  const rf_cn = await rfCN();
  row = await writeBlock(row,"沪深300", pe_hs, rf_cn, null, null, null);  // ERP 取 ERP_TARGET_CN

  // SPX（美国10Y + ERP(US)）
  const pe_spx = await peSPX();
  const rf_us  = await rfUS();
  const { v:erp_us_v, tag:erp_us_tag, link:erp_us_link } = await erpUS();
  row = await writeBlock(row,"标普500", pe_spx, rf_us, erp_us_v, erp_us_tag, erp_us_link);

  console.log("[DONE]", todayStr(), { hs300_pe: pe_hs.v, spx_pe: pe_spx.v });
})();
