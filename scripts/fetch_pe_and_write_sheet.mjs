/**
 * Version History
 * V2.6.9
 *  - 判定修复：用 P/E 与 [买点, 卖点] 区间做判定（P/E ≤ 买点=🟢；P/E ≥ 卖点=🔴；区间内=🟡）
 *  - 邮件 DEBUG：新增 transporter.verify()、发送前后详细日志、FORCE_EMAIL=1 强制发送、完整错误输出
 *  - 不改 daily_valuation.yml；仅更新脚本
 *
 * V2.6.8
 *  - 修复：CSIH30533 的 ROE(TTM) 偶发抓成 30%（专门重写 roeCXIN()，Playwright/HTTP 精准取“ROE …%”并加 3%~40%过滤）
 *
 * V2.6.7
 *  - 去除“中枢（对应P/E上限）”一行；仅保留买点/卖点/合理区间；在“说明（公式）”写清三式
 *
 * V2.6.6
 *  - “指数”行高亮加粗；删除表头行；ROE 百分比、因子小数；日志完整保留
 *
 * V2.6.5
 *  - 清空当日工作表（值+样式+边框）；统一 totalRows 用于写入/格式化/外框；每块后留 1 空行；判定基于“含 ROE 因子”的阈值
 *
 * V2.6.4
 *  - 修复写入范围与实际行数不一致导致 400
 *
 * V2.6.3
 *  - 方案B：新增“合理PE（ROE因子）”；在说明中写明公式；判定基于因子后阈值
 *
 * V2.6.2
 *  - 去除多余 P/E 行；每块加粗浅灰与外框；曾并行显示“原始阈值/ROE因子阈值”
 *
 * V2.6.1 (hotfix)
 *  - 百分比格式修正；ROE(TTM) 抓取增强（Playwright DOM 优先，HTTP/JSON 兜底）
 *
 * V2.6 (Plan B)
 *  - 引入 ROE 因子：PE_limit = 1/(r_f+ERP*) × (ROE/ROE_BASE)；自动抓 ROE(TTM)
 *
 * V2.5
 *  - CSIH30533 切换中国口径：r_f=中国10Y，ERP*=China
 *
 * V2.4
 *  - 新增 CSIH30533 分块；多路鲁棒抓取
 *
 * V2.3
 *  - δ → P/E 空间（买点/卖点/区间三阈值）
 *
 * V2.2
 *  - Nikkei PER 修复；空串不写 0
 *
 * V2.1
 *  - 新增 Nikkei 225（官方档案页）
 *
 * V2.0
 *  - HS300 + SPX 基础版
 */

import fetch from "node-fetch";
import { google } from "googleapis";
import nodemailer from "nodemailer";

// ---------- 全局 ----------
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PW = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";
const DEBUG  = String(process.env.DEBUG_VERBOSE ?? "0") === "1";
const TZ     = process.env.TZ || "Asia/Shanghai";
const dbg    = (...a)=>{ if(DEBUG) console.log("[DEBUG]", ...a); };

const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: TZ }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr = (v,d)=>{ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; };
const strip = (h)=>h.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");

// ---------- 判定参数 ----------
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.005);   // 仅作触发缓冲说明行展示；判定已改为看区间
const ROE_BASE      = numOr(process.env.ROE_BASE,   0.12);    // 12%

// ---------- 兜底 ----------
const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);
const RF_US = numOr(process.env.RF_US,       0.0425);
const RF_JP = numOr(process.env.RF_JP,       0.0100);

const PE_OVERRIDE_CN      = (()=>{ const s=(process.env.PE_OVERRIDE??"").trim();           return s?Number(s):null; })();
const PE_OVERRIDE_SPX     = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim();       return s?Number(s):null; })();
const PE_OVERRIDE_NIKKEI  = (()=>{ const s=(process.env.PE_OVERRIDE_NIKKEI??"").trim();    return s?Number(s):null; })();
const PE_OVERRIDE_CXIN    = (()=>{ const s=(process.env.PE_OVERRIDE_CXIN??"").trim();      return s?Number(s):null; })();

// ---------- Sheets ----------
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if(!SPREADSHEET_ID){ console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL, null,
  (process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version:"v4", auth });

// ========== 工具 ==========
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
async function clearTodaySheet(sheetTitle, sheetId){
  // 清空值（A:Z）+ 样式与边框（避免残留/重复/灰底）
  await sheets.spreadsheets.values.clear({ spreadsheetId:SPREADSHEET_ID, range:`'${sheetTitle}'!A:Z` });
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        { repeatCell: { range:{ sheetId, startRowIndex:0, endRowIndex:2000, startColumnIndex:0, endColumnIndex:26 }, cell:{ userEnteredFormat:{} }, fields:"userEnteredFormat" } },
        { updateBorders: { range:{ sheetId, startRowIndex:0, endRowIndex:2000, startColumnIndex:0, endColumnIndex:26 },
          top:{style:"NONE"}, bottom:{style:"NONE"}, left:{style:"NONE"}, right:{style:"NONE"},
          innerHorizontal:{style:"NONE"}, innerVertical:{style:"NONE"} } }
      ]
    }
  });
}

// ---------- r_f ----------
async function rfCN() {
  try{
    const url="https://cn.investing.com/rates-bonds/china-10-year-bond-yield";
    const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
    if(r.ok){
      const h=await r.text(); let v=null;
      const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
      if(!Number.isFinite(v)){
        const t=strip(h);
        const near=t.match(/(收益率|Yield)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) || t.match(/(\d{1,2}\.\d{1,4})\s*%/);
        if(near) v=Number(near[2]||near[1])/100;
      }
      if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","CN 10Y (Investing)")` };
    }
  }catch{}
  return { v: RF_CN, tag:"兜底", link:"—" };
}
async function rfUS() {
  const urls=[ "https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield","https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield" ];
  for(const url of urls){
    try{
      const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
      if(!r.ok) continue;
      const h=await r.text(); let v=null;
      const m1=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m1) v=Number(m1[1])/100;
      if(!Number.isFinite(v)){
        const t=strip(h); const m2=t.match(/(Yield|收益率)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) || t.match(/(\d{1,2}\.\d{1,4})\s*%/);
        if(m2) v=Number(m2[2]||m2[1])/100;
      }
      if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","US 10Y (Investing)")` };
    }catch{}
  }
  return { v: RF_US, tag:"兜底", link:"—" };
}
async function rfJP() {
  try{
    const url="https://cn.investing.com/rates-bonds/japan-10-year-bond-yield";
    const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
    if(r.ok){
      const h=await r.text(); let v=null;
      const m1=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m1) v=Number(m1[1])/100;
      if(!Number.isFinite(v)){
        const t=strip(h); const m2=t.match(/(Yield|收益率)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) || t.match(/(\d{1,2}\.\d{1,4})\s*%/);
        if(m2) v=Number(m2[2]||m2[1])/100;
      }
      if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","JP 10Y (Investing)")` };
    }
  }catch{}
  return { v: RF_JP, tag:"兜底", link:"—" };
}

// ---------- ERP* ----------
async function erpFromDamodaran(countryRegex, fallbackPct){
  const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(!r.ok) throw 0;
    const html=await r.text();
    const row=html.split(/<\/tr>/i).find(tr=> new RegExp(countryRegex,"i").test(tr)) || "";
    const text=row.replace(/<[^>]+>/g," ");
    const pcts=[...text.matchAll(/(\d{1,2}\.\d{1,2})\s*%/g)].map(m=>Number(m[1]));
    const cand=pcts.find(x=> x>2 && x<10);
    if(cand!=null) return { v:cand/100, tag:"真实", link:`=HYPERLINK("${url}", "Damodaran(${countryRegex})")` };
  }catch{}
  return { v: fallbackPct, tag: "兜底",
           link: `=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")` };
}
async function erpUS(){ return erpFromDamodaran("United\\s*States|USA", 0.0433); }
async function erpJP(){  return erpFromDamodaran("^\\s*Japan\\s*$|Japan", 0.0527); }
async function erpCN(){  return erpFromDamodaran("^\\s*China\\s*$|China", 0.0527); }

// ========== P/E 抓取（稳健实现） ==========
async function peHS300(){
  const url="https://danjuanfunds.com/index-detail/SH000300";
  try{
    if(USE_PW){
      const { chromium }=await import("playwright");
      const br=await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx=await br.newContext({ userAgent:UA, locale:'zh-CN', timezoneId:TZ }); const pg=await ctx.newPage();
      await pg.goto(url,{ waitUntil:'domcontentloaded' }); await pg.waitForTimeout(3000);
      let text=await pg.locator("body").innerText().catch(()=> ""); let m=text && text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(m){ const v=Number(m[1]); await br.close(); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; }
      const v2=await pg.evaluate(()=>{ const re=/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i; for(const el of Array.from(document.querySelectorAll("body *"))){ const t=(el.textContent||"").trim(); if(/分位/.test(t)) continue; const m=t.match(re); if(m) return parseFloat(m[1]); } return null; }).catch(()=> null);
      await br.close(); if(Number.isFinite(v2)&&v2>0&&v2<1000) return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` };
    }
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:12000 });
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let m=text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/); if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; }
      const j=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i); if(j){ const v=Number(j[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; }
    }
  }catch{}
  if(PE_OVERRIDE_CN!=null) return { v:PE_OVERRIDE_CN, tag:"兜底", link:`=HYPERLINK("${url}","Danjuan")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Danjuan")` };
}
async function peSPX(){
  const urlIdx="https://danjuanfunds.com/index-detail/SP500", urlVal="https://danjuanfunds.com/dj-valuation-table-detail/SP500";
  try{
    if(USE_PW){
      const { chromium }=await import("playwright");
      const br=await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx=await br.newContext({ userAgent:UA, locale:'zh-CN', timezoneId:TZ }); const pg=await ctx.newPage();
      await pg.goto(urlIdx,{ waitUntil:'domcontentloaded' }); await pg.waitForTimeout(3000);
      let text=await pg.locator("body").innerText().catch(()=> ""); let m=text && text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(m){ const v=Number(m[1]); await br.close(); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${urlIdx}","Danjuan SP500")` }; }
      const v2=await pg.evaluate(()=>{ const re=/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i; for(const el of Array.from(document.querySelectorAll("body *"))){ const t=(el.textContent||"").trim(); if(/分位/.test(t)) continue; const m=t.match(re); if(m) return parseFloat(m[1]); } return null; }).catch(()=> null);
      await br.close(); if(Number.isFinite(v2)&&v2>0&&v2<1000) return { v:v2, tag:"真实", link:`=HYPERLINK("${urlIdx}","Danjuan SP500")` };
    }
    const r=await fetch(urlVal,{ headers:{ "User-Agent":UA }, timeout:12000 });
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let m=text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/); if(!m) m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` }; }
    }
  }catch{}
  if(PE_OVERRIDE_SPX!=null) return { v:PE_OVERRIDE_SPX, tag:"兜底", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
}
async function peNikkei(){
  const url="https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per";
  try{
    if(USE_PW){
      const { chromium }=await import("playwright");
      const br=await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx=await br.newContext({ userAgent:UA, locale:'en-US', timezoneId:TZ }); const pg=await ctx.newPage();
      await pg.goto(url,{ waitUntil:'domcontentloaded' }); await pg.waitForTimeout(1500);
      const v=await pg.evaluate(()=>{ const tbl=document.querySelector("table"); if(!tbl) return null; const rows=tbl.querySelectorAll("tbody tr"); const row=rows[rows.length-1]; if(!row) return null; const tds=row.querySelectorAll("td"); if(tds.length<3) return null; const txt=(tds[2].textContent||"").trim().replace(/,/g,""); const n=parseFloat(txt); return Number.isFinite(n)? n:null; });
      await br.close(); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
    }
    const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const trs=[...h.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)].map(m=>m[1]); let lastVal=null;
      for(const tr of trs){
        const tds=[...tr.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map(m=>m[1].replace(/<[^>]*>/g,"").trim());
        if(tds.length>=3 && /[A-Za-z]{3}\/\d{2}\/\d{4}/.test(tds[0])){
          const n=parseFloat(tds[2].replace(/,/g,"")); if(Number.isFinite(n)) lastVal=n;
        }
      }
      if(Number.isFinite(lastVal)&&lastVal>0&&lastVal<1000) return { v:lastVal, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
    }
  }catch{}
  if(PE_OVERRIDE_NIKKEI!=null) return { v:PE_OVERRIDE_NIKKEI, tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
}
async function peChinaInternet(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533";
  try{
    if(USE_PW){
      const { chromium }=await import("playwright");
      const br=await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx=await br.newContext({ userAgent:UA, locale:'zh-CN', timezoneId:TZ }); const pg=await ctx.newPage();
      await pg.goto(url,{ waitUntil:'domcontentloaded' }); await pg.waitForTimeout(1800);
      let body=await pg.locator("body").innerText().catch(()=> ""); let m=body && body.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(m){ const v=Number(m[1]); await br.close(); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` }; }
      const v2=await pg.evaluate(()=>{ const bad=(t)=>/分位|百分位|%/.test(t); const re=/(\d{1,3}\.\d{1,2})/; let best=null; for(const el of Array.from(document.querySelectorAll("body *"))){ const t=(el.textContent||"").trim(); if(!/PE\b/i.test(t)) continue; if(bad(t)) continue; const m=t.match(re); if(m){ const x=parseFloat(m[1]); if(Number.isFinite(x)) best=x; } } return best; }).catch(()=> null);
      await br.close(); if(Number.isFinite(v2)&&v2>0&&v2<1000) return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
    }
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let m=text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` }; }
      let j=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(j){ const v=Number(j[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` }; }
    }
  }catch{}
  if(PE_OVERRIDE_CXIN!=null) return { v:PE_OVERRIDE_CXIN, tag:"兜底", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
}

// ========== ROE(TTM) 抓取 ==========
async function roeFromDanjuan(urls){
  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      for(const url of urls){
        await pg.goto(url, { waitUntil: 'domcontentloaded' });
        await pg.waitForTimeout(1500);
        const body = await pg.locator("body").innerText().catch(()=> "");
        let m = body && body.match(/ROE[^%\d]{0,20}(\d{1,2}(?:\.\d{1,2})?)\s*%/i);
        if(m){ const v=Number(m[1])/100; await br.close(); return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
        const v2 = await pg.evaluate(()=>{ const re=/(\d{1,2}(?:\.\d{1,2})?)\s*%/; for(const el of Array.from(document.querySelectorAll("body *"))){ const t=(el.textContent||"").trim(); if(!/ROE\b/i.test(t)) continue; const m=t.match(re); if(m){ const x=parseFloat(m[1]); if(Number.isFinite(x)) return x/100; } } return null; }).catch(()=> null);
        if(Number.isFinite(v2)) { await br.close(); return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      }
      await br.close();
    }catch{}
  }
  for(const url of urls){
    try{
      const r = await fetch(url, { headers:{ "User-Agent": UA }, timeout:15000 });
      if(!r.ok) continue;
      const h = await r.text(); const text = strip(h);
      let j = h.match(/"roe_ttm"\s*:\s*"?(\d{1,2}(?:\.\d{1,2})?)"?/i) || h.match(/"roe"\s*:\s*"?(\d{1,2}(?:\.\d{1,2})?)"?/i);
      if(j){ const v = Number(j[1])/100; if(v>0.03 && v<0.40) return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      const idx = text.search(/ROE\b/i);
      if(idx>=0){
        const right = text.slice(idx, idx+200);
        const m = right.match(/(\d{1,2}(?:\.\d{1,2})?)\s*%/);
        if(m){ const v = Number(m[1])/100; if(v>0.03 && v<0.40) return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      }
    }catch{}
  }
  return { v:"", tag:"兜底", link:"—" };
}

// 专用：中概互联网 ROE（更严格）
async function roeCXIN(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533";
  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(url, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(1600);
      const val = await pg.evaluate(()=>{
        const body = document.body;
        const walker = document.createTreeWalker(body, NodeFilter.SHOW_TEXT);
        let best = null;
        while(walker.nextNode()){
          const t = walker.currentNode.nodeValue.trim();
          if(/^ROE\b/i.test(t)){
            const p = walker.currentNode.parentElement;
            const txt = (p && p.textContent || "").replace(/\s+/g," ");
            const m = txt.match(/ROE[^%\d]{0,40}?(\d{1,2}(?:\.\d{1,2})?)\s*%/i);
            if(m){ best = parseFloat(m[1]); break; }
          }
        }
        return best;
      });
      await br.close();
      if(Number.isFinite(val) && val>3 && val<40) return { v: val/100, tag:"真实", link:`=HYPERLINK("${url}","ROE")` };
    }catch{}
  }
  try{
    const r=await fetch(url,{ headers:{ "User-Agent": UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let j = h.match(/"roe_ttm"\s*:\s*"?(\d{1,2}(?:\.\d{1,2})?)"?/i) || h.match(/"roe"\s*:\s*"?(\d{1,2}(?:\.\d{1,2})?)"?/i);
      if(j){ const v = Number(j[1])/100; if(v>0.03 && v<0.40) return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      const idx = text.search(/ROE\b/i);
      if(idx>=0){
        const right = text.slice(idx, idx+200);
        const m = right.match(/(\d{1,2}(?:\.\d{1,2})?)\s*%/);
        if(m){ const v = Number(m[1])/100; if(v>0.03 && v<0.40) return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      }
    }
  }catch{}
  return { v:"", tag:"兜底", link:"—" };
}

// ---------- 写单块（改：判定用 P/E 与区间） ----------
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink, roeRes){
  const { sheetTitle, sheetId } = await ensureToday();

  const pe = (peRes==null || peRes.v==="" || peRes.v==null) ? null : Number(peRes.v);
  const rf = Number.isFinite(rfRes?.v) ? rfRes.v : null;
  const target = (label==="沪深300") ? ERP_TARGET_CN : erpStar;
  const roe = Number.isFinite(roeRes?.v) ? roeRes.v : null;    // 小数（0.xx）

  const ep = Number.isFinite(pe) ? 1/pe : null;

  // ROE 因子（显示为小数，不带百分号）
  const factor = (roe!=null && roe>0) ? (roe/ROE_BASE) : 1;
  const factorDisp = (roe!=null && roe>0) ? Number(factor.toFixed(2)) : "";

  // 仅保留“因子后”的买/卖/区间
  const peBuy  = (rf!=null && target!=null) ? Number((1/(rf+target+DELTA)*factor).toFixed(2)) : null;
  const peSell = (rf!=null && target!=null && (rf+target-DELTA)>0) ? Number((1/(rf+target-DELTA)*factor).toFixed(2)) : null;
  const fairRange = (peBuy!=null && peSell!=null) ? `${peBuy} ~ ${peSell}` : "";

  // 判定（**改为基于 P/E 与区间**）
  let status="需手动更新";
  if(Number.isFinite(pe) && peBuy!=null && peSell!=null){
    if (pe <= peBuy) status="🟢 买点（低估）";
    else if (pe >= peSell) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  // 写入值（“指数”为首行）
  const values = [
    ["指数", label, "真实", "宽基/行业指数估值分块", peRes?.link || "—"],                            // 0 高亮加粗
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peRes?.tag || (Number.isFinite(pe)?"真实":"兜底"), "估值来源", peRes?.link || "—"], // 1
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],                             // 2
    ["无风险利率 r_f（10Y名义）", rf ?? "", rf!=null?"真实":"兜底", (label==="沪深300"?"有知有行 10Y":"Investing.com 10Y"), rfRes?.link || "—"], // 3
    ["目标 ERP*", (label==="沪深300"? ERP_TARGET_CN : (Number.isFinite(target)?target:"")), (label==="沪深300"?"真实":(Number.isFinite(target)?"真实":"兜底")),
      (label==="沪深300"?"建议参考达摩达兰":"达摩达兰"), erpLink || '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'], // 4
    ["容忍带 δ", DELTA, "真实", "减少频繁切换（触发缓冲说明，不参与卖点定义）","—"],                                                         // 5
    ["买点PE上限（含ROE因子）", peBuy ?? "", (peBuy!=null)?"真实":"兜底", "买点=1/(r_f+ERP*+δ)×factor","—"],                               // 6
    ["卖点PE下限（含ROE因子）", peSell ?? "", (peSell!=null)?"真实":"兜底", "卖点=1/(r_f+ERP*−δ)×factor","—"],                              // 7
    ["合理PE区间（含ROE因子）", fairRange, (peBuy!=null && peSell!=null)?"真实":"兜底", "买点上限 ~ 卖点下限","—"],                           // 8
    ["ROE（TTM）", roe ?? "", (roe!=null)?"真实":"兜底", "盈利能力（小数，显示为百分比）", roeRes?.link || "—"],                               // 9
    ["ROE基准（可配 env.ROE_BASE）", ROE_BASE, "真实", "默认 0.12 = 12%","—"],                                                            //10
    ["ROE倍数因子 = ROE/ROE基准", factorDisp, (factorDisp!=="")?"真实":"兜底", "例如 16.4%/12% = 1.36","—"],                                  //11
    ["说明（公式）", "见右", "真实", "买点=1/(r_f+ERP*+δ)×factor；卖点=1/(r_f+ERP*−δ)×factor；合理区间=买点~卖点","—"],                           //12
    ["判定", status, (Number.isFinite(pe) && peBuy!=null && peSell!=null)?"真实":"兜底", "基于 P/E 与区间判定","—"],                         //13
  ];

  const totalRows = values.length;
  const endRow = startRow + totalRows - 1;

  await write(`'${sheetTitle}'!A${startRow}:E${endRow}`, values);

  // —— 格式化 ——（指数行为首行）
  const base = startRow - 1; // 0-based
  // 百分比：E/P(2)、r_f(3)、ERP*(4)、δ(5)、ROE(9)、ROE基准(10)
  const pctRowsAbs = [base+2, base+3, base+4, base+5, base+9, base+10];
  // 数字：P/E(1)、买点(6)、卖点(7)、因子(11)
  const numberRowsAbs = [base+1, base+6, base+7, base+11];

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        ...pctRowsAbs.map(r => ({
          repeatCell: {
            range: { sheetId, startRowIndex:r, endRowIndex:r+1, startColumnIndex:1, endColumnIndex:2 },
            cell: { userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } },
            fields: "userEnteredFormat.numberFormat"
          }
        })),
        ...numberRowsAbs.map(r => ({
          repeatCell: {
            range: { sheetId, startRowIndex:r, endRowIndex:r+1, startColumnIndex:1, endColumnIndex:2 },
            cell: { userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } },
            fields: "userEnteredFormat.numberFormat"
          }
        })),
        // “指数”首行加粗 + 浅灰底
        {
          repeatCell: {
            range: { sheetId, startRowIndex: base+0, endRowIndex: base+1, startColumnIndex:0, endColumnIndex:5 },
            cell: { userEnteredFormat:{ backgroundColor:{ red:0.95, green:0.95, blue:0.95 }, textFormat:{ bold:true } } },
            fields: "userEnteredFormat(backgroundColor,textFormat)"
          }
        },
        // 分块外框
        {
          updateBorders: {
            range: { sheetId, startRowIndex: base, endRowIndex: base + totalRows, startColumnIndex:0, endColumnIndex:5 },
            top:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
            bottom:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
            left:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
            right:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } }
          }
        }
      ]
    }
  });

  // 下一块（留 1 行空白）
  return endRow + 2;
}

// ========== 邮件发送（加入 DEBUG/verify/FORCE_EMAIL） ==========
async function sendEmailIfEnabled(summaryHtml){
  const {
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
    MAIL_TO, MAIL_FROM_NAME,
    FORCE_EMAIL
  } = process.env;

  if(!SMTP_HOST || !SMTP_PORT || !SMTP_USER || !SMTP_PASS || !MAIL_TO){
    dbg("[MAIL] skip: SMTP env not complete", { SMTP_HOST, SMTP_PORT, MAIL_TO });
    return;
  }

  const transporter = nodemailer.createTransport({
    host: SMTP_HOST,
    port: Number(SMTP_PORT),
    secure: Number(SMTP_PORT) === 465,
    auth: { user: SMTP_USER, pass: SMTP_PASS }
  });

  try{
    dbg("[MAIL] verify start", { host: SMTP_HOST, user: SMTP_USER, to: MAIL_TO });
    await transporter.verify();
    dbg("[MAIL] verify ok");
  }catch(e){
    console.error("[MAIL] verify fail:", e);
    if(!FORCE_EMAIL) return; // 如需强制测试，设置 FORCE_EMAIL=1
    console.error("[MAIL] continue due to FORCE_EMAIL=1");
  }

  const subject = `Valuation Daily — ${todayStr()} (${TZ})`;
  const from = MAIL_FROM_NAME ? `${MAIL_FROM_NAME} <${SMTP_USER}>` : SMTP_USER;
  const mailOptions = {
    from, to: MAIL_TO,
    subject,
    html: summaryHtml || `<p>Valuation daily finished at ${todayStr()} (${TZ}).</p>`
  };

  dbg("[MAIL] send start", { subject, to: MAIL_TO });
  try{
    const info = await transporter.sendMail(mailOptions);
    console.log("[MAIL] sent", { messageId: info.messageId, response: info.response });
  }catch(e){
    console.error("[MAIL] send error:", e);
  }
}

// ========== Main ==========
(async()=>{
  console.log("[INFO] Run start", todayStr(), "USE_PLAYWRIGHT=", USE_PW, "TZ=", TZ);

  let row=1;

  const { sheetTitle, sheetId } = await ensureToday();
  await clearTodaySheet(sheetTitle, sheetId);

  // 1) HS300（中国口径）
  const pe_hs = await peHS300();        const rf_cn  = await rfCN();
  const roe_hs = await roeFromDanjuan(["https://danjuanfunds.com/index-detail/SH000300"]);
  row = await writeBlock(row,"沪深300", pe_hs, rf_cn, null, null, null, roe_hs);

  // 2) SP500（美国口径）
  const rf_us  = await rfUS();
  const { v:erp_us_v, tag:erp_us_tag, link:erp_us_link } = await erpUS();
  const pe_spx = await peSPX();         const roe_spx = await roeFromDanjuan(["https://danjuanfunds.com/dj-valuation-table-detail/SP500","https://danjuanfunds.com/index-detail/SP500"]);
  row = await writeBlock(row,"标普500", pe_spx, rf_us, erp_us_v, erp_us_tag, erp_us_link, roe_spx);

  // 3) Nikkei（日本口径；无 ROE → 因子=1）
  const pe_nk = await peNikkei();       const rf_jp  = await rfJP();
  const { v:erp_jp_v, tag:erp_jp_tag, link:erp_jp_link } = await erpJP();
  row = await writeBlock(row,"日经指数", pe_nk, rf_jp, erp_jp_v, erp_jp_tag, erp_jp_link, null);

  // 4) 中概互联网（中国口径，专用 ROE 抓取）
  const pe_cxin = await peChinaInternet(); const rf_cn2  = await rfCN();
  const { v:erp_cn_v, tag:erp_cn_tag, link:erp_cn_link } = await erpCN();
  const roe_cxin = await roeCXIN();
  row = await writeBlock(row,"中概互联网", pe_cxin, rf_cn2, erp_cn_v, erp_cn_tag, erp_cn_link, roe_cxin);

  console.log("[DONE]", todayStr(), { hs300_pe: pe_hs?.v, spx_pe: pe_spx?.v, nikkei_pe: pe_nk?.v, cxin_pe: pe_cxin?.v });

  // （可选）邮件摘要很简单：把四个 P/E 与区间拼一下；实际你可以用更丰富的 HTML
  const summary = `
    <h3>Valuation Daily — ${todayStr()} (${TZ})</h3>
    <ul>
      <li>HS300 PE: ${pe_hs?.v ?? "-"} </li>
      <li>SPX   PE: ${pe_spx?.v ?? "-"} </li>
      <li>Nikkei PE: ${pe_nk?.v ?? "-"} </li>
      <li>China Internet PE: ${pe_cxin?.v ?? "-"}</li>
    </ul>
    <p>See sheet "${sheetTitle}" for thresholds & judgments.</p>
  `;
  await sendEmailIfEnabled(summary);
})();
