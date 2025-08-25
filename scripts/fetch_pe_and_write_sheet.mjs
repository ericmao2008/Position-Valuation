/**
 * Version History
 * V2.6.3
 *  - 简化：只保留“方案B：ROE倍数因子”后的单套阈值（中枢/买点/卖点/区间）
 *  - 新增：ROE倍数因子 = ROE / ROE_BASE；“说明（公式）”写清计算原理
 *  - 判定：改为以“ROE因子后的阈值”直接比较当前P/E
 *  - 视觉：保留分块加粗灰底与外框；避免任何 P/E 被设为百分比
 *
 * V2.6.2
 *  - 去除重复 P/E；区隔样式；曾并行显示“原始阈值”和“ROE因子阈值”
 *
 * V2.6.1
 *  - 百分比格式修正；ROE(TTM) 抓取增强
 *
 * V2.6
 *  - 方案B：合理PE（ROE因子） = 1/(r_f+ERP*) × (ROE/ROE_BASE)
 *
 * V2.5
 *  - 中概互联网：r_f=中国10Y；ERP*=China
 *
 * V2.4
 *  - 新增中概互联网（CSIH30533）
 *
 * V2.3
 *  - δ → P/E 空间三阈值
 *
 * V2.2
 *  - Nikkei PER + 空串不写0
 *
 * V2.1
 *  - 新增 Nikkei 225
 *
 * V2.0
 *  - HS300 + SPX 基础版
 */

import fetch from "node-fetch";
import { google } from "googleapis";

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
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);   // HS300（可覆盖）
const DELTA         = numOr(process.env.DELTA,      0.005);
const ROE_BASE      = numOr(process.env.ROE_BASE,   0.12);     // 12%

// ---------- 兜底（小数） ----------
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

// ---------- r_f ----------
async function rfCN() {
  dbg("rfCN start (Investing first)");
  try {
    const url = "https://cn.investing.com/rates-bonds/china-10-year-bond-yield";
    const r = await fetch(url, { headers: { "User-Agent": UA, "Referer": "https://www.google.com" }, timeout: 12000 });
    dbg("rfCN investing status", r.status);
    if (r.ok) {
      const h = await r.text();
      let m = h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i);
      let v = m ? Number(m[1]) / 100 : null;
      if (!Number.isFinite(v)) {
        const text = strip(h);
        const near = text.match(/(收益率|Yield)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) ||
                     text.match(/(\d{1,2}\.\d{1,4})\s*%/);
        if (near) v = Number(near[2] || near[1]) / 100;
        dbg("rfCN regex pct near", v);
      }
      if (Number.isFinite(v) && v > 0 && v < 1)
        return { v, tag: "真实", link: `=HYPERLINK("${url}","CN 10Y (Investing)")` };
    }
  } catch (e) { dbg("rfCN investing err", e.message); }
  dbg("rfCN fallback", RF_CN);
  return { v: RF_CN, tag: "兜底", link: "—" };
}

async function rfUS() {
  dbg("rfUS start (Investing)");
  const urls = [
    "https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield",
    "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"
  ];
  for (const url of urls) {
    try {
      const r = await fetch(url, { headers: { "User-Agent": UA, "Referer": "https://www.google.com" }, timeout: 12000 });
      dbg("rfUS status", url, r.status);
      if (!r.ok) continue;
      const h = await r.text();
      let v = null;
      const m1 = h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i);
      if (m1) v = Number(m1[1]) / 100;
      if (!Number.isFinite(v)) {
        const text = strip(h);
        const m2 = text.match(/(Yield|收益率)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) || text.match(/(\d{1,2}\.\d{1,4})\s*%/);
        if (m2) v = Number(m2[2] || m2[1]) / 100;
      }
      if (Number.isFinite(v) && v > 0 && v < 1)
        return { v, tag: "真实", link: `=HYPERLINK("${url}","US 10Y (Investing)")` };
    } catch (e) { dbg("rfUS err", e.message); }
  }
  dbg("rfUS fallback", RF_US);
  return { v: RF_US, tag: "兜底", link: "—" };
}

async function rfJP() {
  dbg("rfJP start (Investing)");
  const url = "https://cn.investing.com/rates-bonds/japan-10-year-bond-yield";
  try {
    const r = await fetch(url, { headers: { "User-Agent": UA, "Referer": "https://www.google.com" }, timeout: 12000 });
    dbg("rfJP status", r.status);
    if (r.ok) {
      const h = await r.text();
      let v = null;
      const m1 = h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i);
      if (m1) v = Number(m1[1]) / 100;
      if (!Number.isFinite(v)) {
        const text = strip(h);
        const m2 = text.match(/(Yield|收益率)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) || text.match(/(\d{1,2}\.\d{1,4})\s*%/);
        if (m2) v = Number(m2[2] || m2[1]) / 100;
      }
      if (Number.isFinite(v) && v > 0 && v < 1)
        return { v, tag: "真实", link: `=HYPERLINK("${url}","JP 10Y (Investing)")` };
    }
  } catch (e) { dbg("rfJP err", e.message); }
  dbg("rfJP fallback", RF_JP);
  return { v: RF_JP, tag: "兜底", link: "—" };
}

// ---------- ERP*(通用) ----------
async function erpFromDamodaran(countryRegex, fallbackPct){
  dbg("erp* start", countryRegex);
  const url = "https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  try{
    const r = await fetch(url, { headers:{ "User-Agent": UA }, timeout: 15000 });
    dbg("erp* status", r.status);
    if(!r.ok) throw new Error("status not ok");
    const html = await r.text();

    const row  = html.split(/<\/tr>/i).find(tr => new RegExp(countryRegex, "i").test(tr)) || "";
    const text = row.replace(/<[^>]+>/g, " ");
    const pcts = [...text.matchAll(/(\d{1,2}\.\d{1,2})\s*%/g)].map(m => Number(m[1]));
    dbg("erp* row pcts", countryRegex, pcts);
    const candidate = pcts.find(x => x > 2 && x < 10);
    if (candidate != null) return { v:candidate/100, tag:"真实", link:`=HYPERLINK("${url}", "Damodaran(${countryRegex})")` };
  }catch(e){
    dbg("erp* error", e.message);
  }
  return { v: fallbackPct, tag: "兜底",
           link: `=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")` };
}
async function erpUS(){ return erpFromDamodaran("United\\s*States|USA", 0.0433); }
async function erpJP(){  return erpFromDamodaran("^\\s*Japan\\s*$|Japan", 0.0527); }
async function erpCN(){  return erpFromDamodaran("^\\s*China\\s*$|China", 0.0527); }

// ========== Danjuan：PE 抓取（与 V2.6.2 同，略小改确保稳定） ==========
async function peHS300(){
  const url = "https://danjuanfunds.com/index-detail/SH000300";

  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();

      await pg.goto(url, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(3000);

      let text = await pg.locator("body").innerText().catch(()=> "");
      dbg("HS300 index-detail body len", text?.length || 0);
      let val  = null;
      let m = text && text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if (m) val = Number(m[1]);

      if (!Number.isFinite(val)) {
        val = await pg.evaluate(() => {
          const re = /PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i;
          for (const el of Array.from(document.querySelectorAll("body *"))) {
            const t = (el.textContent || "").trim();
            if (/分位/.test(t)) continue;
            const m = t.match(re);
            if (m) return parseFloat(m[1]);
          }
          return null;
        }).catch(()=> null);
      }

      await br.close();
      if (Number.isFinite(val) && val > 0 && val < 1000) {
        return { v: val, tag: "真实", link: `=HYPERLINK("${url}","Danjuan")` };
      }
    } catch (e) { dbg("HS300 index-detail PW error", e.message); }
  }

  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:12000 });
    dbg("HS300 HTTP status", r.status);
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let m = text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if (m) {
        const v=Number(m[1]); dbg("HS300 HTTP regex", v);
        if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` };
      }
      const mJson=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){
        const v=Number(mJson[1]); dbg("HS300 HTTP json pe_ttm", v);
        if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` };
      }
    }
  }catch(e){ dbg("HS300 HTTP error", e.message); }

  if(PE_OVERRIDE_CN!=null) return { v:PE_OVERRIDE_CN, tag:"兜底", link:`=HYPERLINK("${url}","Danjuan")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Danjuan")` };
}

async function peSPX(){
  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      const url = "https://danjuanfunds.com/index-detail/SP500";
      await pg.goto(url, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(3000);

      let text = await pg.locator("body").innerText().catch(()=> "");
      dbg("SPX index-detail body len", text?.length || 0);
      let m = text && text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(m){ const v=Number(m[1]); await br.close();
        if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan SP500")` }; }
      const v2 = await pg.evaluate(()=>{
        const re=/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i;
        for(const el of Array.from(document.querySelectorAll("body *"))){
          const t=(el.textContent||"").trim();
          if(/分位/.test(t)) continue;
          const m=t.match(re);
          if(m) return parseFloat(m[1]);
        }
        return null;
      }).catch(()=> null);
      await br.close();
      if(Number.isFinite(v2)&&v2>0&&v2<1000) return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","Danjuan SP500")` };
    }catch(e){ dbg("SPX index-detail PW error", e.message); }
  }

  const urlVal = "https://danjuanfunds.com/dj-valuation-table-detail/SP500";
  try{
    const r=await fetch(urlVal,{ headers:{ "User-Agent":UA }, timeout:12000 });
    dbg("SPX valuation HTTP status", r.status);
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let m=text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(!m) m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(m){
        const v=Number(m[1]); dbg("SPX valuation regex/json", v);
        if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
      }
    }
  }catch(e){ dbg("SPX valuation HTTP err", e.message); }

  if(PE_OVERRIDE_SPX!=null) return { v:PE_OVERRIDE_SPX, tag:"兜底", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
}

async function peNikkei(){
  const url = "https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per";

  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'en-US', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(url, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(1500);

      const v = await pg.evaluate(()=>{
        const tbl = document.querySelector("table");
        if(!tbl) return null;
        const rows = tbl.querySelectorAll("tbody tr");
        const row = rows[rows.length - 1];
        if(!row) return null;
        const tds = row.querySelectorAll("td");
        if(tds.length < 3) return null;
        const txt = (tds[2].textContent||"").trim().replace(/,/g,"");
        const n = parseFloat(txt);
        return Number.isFinite(n) ? n : null;
      });
      await br.close();
      if(Number.isFinite(v) && v>0 && v<1000){
        return { v, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
      }
    }catch(e){ dbg("Nikkei PW error", e.message); }
  }

  try{
    const r = await fetch(url, { headers:{ "User-Agent": UA, "Referer":"https://www.google.com" }, timeout:15000 });
    dbg("Nikkei page status", r.status);
    if(r.ok){
      const h = await r.text();
      const trs = [...h.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)].map(m=>m[1]);
      let lastVal = null;
      for(const tr of trs){
        const tds = [...tr.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map(m=>m[1].replace(/<[^>]*>/g,"").trim());
        if(tds.length>=3 && /[A-Za-z]{3}\/\d{2}\/\d{4}/.test(tds[0])){
          const n = parseFloat(tds[2].replace(/,/g,""));
          if(Number.isFinite(n)) lastVal = n;
        }
      }
      if(Number.isFinite(lastVal) && lastVal>0 && lastVal<1000){
        return { v:lastVal, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
      }
    }
  }catch(e){ dbg("Nikkei fetch error", e.message); }

  if(PE_OVERRIDE_NIKKEI!=null) return { v: PE_OVERRIDE_NIKKEI, tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
}

async function peChinaInternet(){
  const url = "https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533";

  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(url, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(1800);

      let bodyText = await pg.locator("body").innerText().catch(()=> "");
      let m = bodyText && bodyText.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if (m) {
        const v = Number(m[1]); await br.close();
        if(Number.isFinite(v) && v>0 && v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
      }
      const v2 = await pg.evaluate(()=>{
        const isBad = (t)=> /分位|百分位|%/.test(t);
        const reNum = /(\d{1,3}\.\d{1,2})/;
        let best = null;
        for(const el of Array.from(document.querySelectorAll("body *"))){
          const t = (el.textContent||"").trim();
          if(!/PE\b/i.test(t)) continue;
          if(isBad(t)) continue;
          const m = t.match(reNum);
          if(m){ const x = parseFloat(m[1]); if(Number.isFinite(x)) best = x; }
        }
        return best;
      }).catch(()=> null);
      await br.close();
      if(Number.isFinite(v2) && v2>0 && v2<1000) return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
    }catch(e){ dbg("CSIH30533 PW error", e.message); }
  }

  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    dbg("CSIH30533 HTTP status", r.status);
    if(r.ok){
      const h=await r.text();
      const text=strip(h);

      let m = text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(m){
        const v=Number(m[1]);
        if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
      }
      let mJson = h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){
        const v=Number(mJson[1]);
        if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
      }
    }
  }catch(e){ dbg("CSIH30533 HTTP error", e.message); }

  if(PE_OVERRIDE_CXIN!=null) return { v:PE_OVERRIDE_CXIN, tag:"兜底", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
}

// ========== ROE(TTM) 抓取 ==========
async function roeFromDanjuan(urls){
  // Playwright 优先（如可用）
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
        const v2 = await pg.evaluate(()=>{
          const rePct = /(\d{1,2}(?:\.\d{1,2})?)\s*%/;
          for(const el of Array.from(document.querySelectorAll("body *"))){
            const t=(el.textContent||"").trim();
            if(!/ROE\b/i.test(t)) continue;
            const m=t.match(rePct);
            if(m){ const x = parseFloat(m[1]); if(Number.isFinite(x)) return x/100; }
          }
          return null;
        }).catch(()=> null);
        if(Number.isFinite(v2)) { await br.close(); return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      }
      await br.close();
    }catch(e){ dbg("ROE PW err", e.message); }
  }
  // HTTP 回退
  for(const url of urls){
    try{
      const r = await fetch(url, { headers:{ "User-Agent": UA }, timeout:15000 });
      dbg("ROE fetch", url, r.status);
      if(!r.ok) continue;
      const h = await r.text(); const text = strip(h);
      let m = text.match(/ROE[^%\d]{0,20}(\d{1,2}(?:\.\d{1,2})?)\s*%/i);
      if(m){ const v = Number(m[1])/100; return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      m = h.match(/"roe(?:_ttm)?"\s*:\s*"?(\d{1,2}(?:\.\d{1,2})?)"?/i);
      if(m){ const v = Number(m[1])/100; return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
    }catch(e){ dbg("ROE HTTP err", e.message); }
  }
  return { v:"", tag:"兜底", link:"—" };
}
async function roeHS300(){ return roeFromDanjuan(["https://danjuanfunds.com/index-detail/SH000300"]); }
async function roeSPX(){  return roeFromDanjuan(["https://danjuanfunds.com/dj-valuation-table-detail/SP500","https://danjuanfunds.com/index-detail/SP500"]); }
async function roeCXIN(){ return roeFromDanjuan(["https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533"]); }

// ---------- 写单块（仅保留“ROE因子后”的单套阈值 & 视觉区隔） ----------
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink, roeRes){
  const { sheetTitle, sheetId } = await ensureToday();

  const pe = (peRes==null || peRes.v==="" || peRes.v==null) ? null : Number(peRes.v);
  const rf = Number.isFinite(rfRes?.v) ? rfRes.v : null;
  const target = (label==="沪深300") ? ERP_TARGET_CN : erpStar;
  const roe = Number.isFinite(roeRes?.v) ? roeRes.v : null;    // 小数

  const ep = Number.isFinite(pe) ? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rf)) ? (ep - rf) : null; // 供参考，不用于最终判定
  const peLimitBase = (Number.isFinite(rf) && Number.isFinite(target)) ? (1/(rf+target)) : null;
  const buyUpperBase = (Number.isFinite(rf) && Number.isFinite(target)) ? (1/(rf+target+DELTA)) : null;
  const sellLowerBase = (Number.isFinite(rf) && Number.isFinite(target) && (rf+target-DELTA)>0) ? (1/(rf+target-DELTA)) : null;

  // ROE 因子
  const factor = (roe!=null && roe>0) ? (roe/ROE_BASE) : 1;
  const factorDisp = (roe!=null && roe>0) ? Number((factor).toFixed(2)) : "";

  // 只保留“因子后”的阈值（保留两位小数显示）
  const peLimit   = (peLimitBase  !=null) ? Number((peLimitBase  * factor).toFixed(2)) : null;
  const buyUpper  = (buyUpperBase !=null) ? Number((buyUpperBase * factor).toFixed(2)) : null;
  const sellLower = (sellLowerBase!=null) ? Number((sellLowerBase* factor).toFixed(2)) : null;
  const fairRange = (buyUpper!=null && sellLower!=null) ? `${buyUpper} ~ ${sellLower}` : "";

  // 判定：完全以“因子后”阈值为准
  let status="需手动更新";
  if (Number.isFinite(pe) && buyUpper!=null && sellLower!=null) {
    if (pe <= buyUpper) status="🟢 买点（低估）";
    else if (pe >= sellLower) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  // —— 内容行（简化版）——
  const rows = [
    ["指数", label, "真实", "宽基/行业指数估值分块", peRes?.link || "—"],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peRes?.tag || (Number.isFinite(pe)?"真实":"兜底"), "估值来源", peRes?.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rf!=null?"真实":"兜底", (label==="沪深300"?"有知有行 10Y":"Investing.com 10Y"), rfRes?.link || "—"],
    ["目标 ERP*", (label==="沪深300"? ERP_TARGET_CN : (Number.isFinite(target)?target:"")), (label==="沪深300"?"真实":(Number.isFinite(target)?"真实":"兜底")),
      (label==="沪深300"?"建议参考达摩达兰":"达摩达兰"), erpLink || '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    // —— 仅展示“因子后”阈值 ——
    ["对应P/E上限（含ROE因子）", peLimit ?? "", (peLimit!=null)?"真实":"兜底", "中枢：1/(r_f+ERP*)×(ROE/ROE_base)","—"],
    ["买点PE上限（含ROE因子）", buyUpper ?? "", (buyUpper!=null)?"真实":"兜底", "买点：1/(r_f+ERP*+δ)×factor","—"],
    ["卖点PE下限（含ROE因子）", sellLower ?? "", (sellLower!=null)?"真实":"兜底", "卖点：1/(r_f+ERP*−δ)×factor","—"],
    ["合理PE区间（含ROE因子）", fairRange, (buyUpper!=null && sellLower!=null)?"真实":"兜底", "买点上限 ~ 卖点下限","—"],
    // —— ROE & 因子与公式说明 ——
    ["ROE（TTM）", roe ?? "", (roe!=null)?"真实":"兜底", "盈利能力（小数，显示为百分比）", roeRes?.link || "—"],
    ["ROE基准（可配 env.ROE_BASE）", ROE_BASE, "真实", "默认 0.12 = 12%","—"],
    ["ROE倍数因子 = ROE/ROE基准", factorDisp, (factorDisp!=="")?"真实":"兜底", "例如 16.4%/12% = 1.36","—"],
    ["说明（公式）", "见右", "真实", "PE_limit = 1/(r_f+ERP*)×(ROE/ROE_base); 买点=1/(r_f+ERP*+δ)×factor; 卖点=1/(r_f+ERP*−δ)×factor","—"],
    ["判定", status, (Number.isFinite(pe) && buyUpper!=null && sellLower!=null)?"真实":"兜底", "按含ROE因子的阈值判断","—"],
  ];

  // 写入
  const end = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`, [["字段","数值","数据","说明","数据源"], ...rows]);

  // —— 单元格格式化 —— 
  // 百分比：E/P、r_f、ERP*、δ、ROE、ROE基准
  const hdr = 1; // 标题行偏移
  const pctRows = [hdr+2, hdr+3, hdr+5, hdr+6, hdr+10, hdr+11]; // 1-based相对：E/P(3), r_f(4), ERP*(6), δ(7), ROE(11), ROE基准(12)
  const numberRows = [hdr+1, hdr+7, hdr+8, hdr+9, hdr+12]; // P/E、三阈值与因子数值行
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        ...pctRows.map(i => ({
          repeatCell: {
            range: { sheetId, startRowIndex:(startRow-1)+i-1, endRowIndex:(startRow-1)+i, startColumnIndex:1, endColumnIndex:2 },
            cell: { userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } },
            fields: "userEnteredFormat.numberFormat"
          }
        })),
        ...numberRows.map(i => ({
          repeatCell: {
            range: { sheetId, startRowIndex:(startRow-1)+i-1, endRowIndex:(startRow-1)+i, startColumnIndex:1, endColumnIndex:2 },
            cell: { userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } },
            fields: "userEnteredFormat.numberFormat"
          }
        })),
        // 视觉区隔：给“指数”行加粗+浅灰背景（在数据区第2行=总体第 startRow+1）
        {
          repeatCell: {
            range: { sheetId, startRowIndex:(startRow-1)+1, endRowIndex:(startRow-1)+2, startColumnIndex:0, endColumnIndex:5 },
            cell: { userEnteredFormat:{ backgroundColor:{ red:0.95, green:0.95, blue:0.95 }, textFormat:{ bold:true } } },
            fields: "userEnteredFormat(backgroundColor,textFormat)"
          }
        },
        // 分块外框
        {
          updateBorders: {
            range: { sheetId, startRowIndex:(startRow-1), endRowIndex:end, startColumnIndex:0, endColumnIndex:5 },
            top:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
            bottom:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
            left:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
            right:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } }
          }
        }
      ]
    }
  });

  return end + 2; // 分块之间留 1 行空白
}

// ========== Main ==========
(async()=>{
  console.log("[INFO] Run start", todayStr(), "USE_PLAYWRIGHT=", USE_PW, "TZ=", TZ);

  let row=1;

  // 1) 沪深300（中国10Y + ERP_TARGET_CN）
  const pe_hs = await peHS300();
  const rf_cn = await rfCN();
  const roe_hs = await roeHS300();
  row = await writeBlock(row,"沪深300", pe_hs, rf_cn, null, null, null, roe_hs);

  // 2) 标普500（美国10Y + ERP(US)）
  const rf_us  = await rfUS();
  const { v:erp_us_v, tag:erp_us_tag, link:erp_us_link } = await erpUS();
  const pe_spx = await peSPX();
  const roe_spx = await roeSPX();
  row = await writeBlock(row,"标普500", pe_spx, rf_us, erp_us_v, erp_us_tag, erp_us_link, roe_spx);

  // 3) 日经225（日本10Y + ERP(Japan)）
  const pe_nk = await peNikkei();
  const rf_jp = await rfJP();
  const { v:erp_jp_v, tag:erp_jp_tag, link:erp_jp_link } = await erpJP();
  row = await writeBlock(row,"日经指数", pe_nk, rf_jp, erp_jp_v, erp_jp_tag, erp_jp_link, null); // 暂无 ROE → factor=1

  // 4) 中概互联网（CSIH30533：中国10Y + ERP(China)）
  const pe_cxin = await peChinaInternet();
  const rf_cn2  = await rfCN();
  const { v:erp_cn_v, tag:erp_cn_tag, link:erp_cn_link } = await erpCN();
  const roe_cxin = await roeCXIN();
  row = await writeBlock(row,"中概互联网", pe_cxin, rf_cn2, erp_cn_v, erp_cn_tag, erp_cn_link, roe_cxin);

  console.log("[DONE]", todayStr(), {
    hs300_pe: pe_hs?.v, spx_pe: pe_spx?.v, nikkei_pe: pe_nk?.v, cxin_pe: pe_cxin?.v
  });
})();
