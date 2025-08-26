/**
 * Version History
 * V3.0.2 - Subs email format + Moutai price (Google Finance)
 * - Subs (子公司) email line now "Tencent 6.01万亿 🟡 持有".
 * - Add fetchMoutaiPrice() from Google Finance (no Sheets formula).
 * - Write a simple price block for Moutai; also append to email.
 */

import fetch from "node-fetch";
import { google } from "googleapis";
import nodemailer from "nodemailer";
import fs from "fs";
import path from "path";

// ===== Global =====
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PW = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";
const DEBUG  = String(process.env.DEBUG_VERBOSE ?? "0") === "1";
const TZ     = process.env.TZ || "Asia/Shanghai";
const dbg    = (...a)=>{ if(DEBUG) console.log("[DEBUG]", ...a); };

const VC_URL = "https://danjuanfunds.com/djmodule/value-center?channel=1300100141";

// 目标指数
const VC_TARGETS = {
  SH000300: { name: "沪深300", code: "SH000300", country: "CN" },
  SP500:    { name: "标普500", code: "SP500", country: "US" },
  CSIH30533:{ name: "中概互联50", code: "CSIH30533", country: "CN" },
  HSTECH:   { name: "恒生科技", code: "HKHSTECH", country: "CN" },
  NDX:      { name: "纳指100", code: "NDX", country: "US" },
  GDAXI:    { name: "德国DAX", code: "GDAXI", country: "DE" },
};

// ===== Policy / Defaults =====
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.01); 
const ROE_BASE      = numOr(process.env.ROE_BASE,   0.12);

const RF_CN = numOr(process.env.RF_CN, 0.0178);
const RF_US = numOr(process.env.RF_US, 0.0425);
const RF_JP = numOr(process.env.RF_JP, 0.0100);
const RF_DE = numOr(process.env.RF_DE, 0.025);
const RF_IN = numOr(process.env.RF_IN, 0.07);

const PE_OVERRIDE_CN     = (()=>{ const s=(process.env.PE_OVERRIDE_CN??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_SPX    = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_CXIN   = (()=>{ const s=(process.env.PE_OVERRIDE_CXIN??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_HSTECH = (()=>{ const s=(process.env.PE_OVERRIDE_HSTECH??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_NDX    = (()=>{ const s=(process.env.PE_OVERRIDE_NDX??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_DAX    = (()=>{ const s=(process.env.PE_OVERRIDE_DAX??"").trim(); return s?Number(s):null; })();

// ===== Sheets =====
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if(!SPREADSHEET_ID){ console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL, null,
  (process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version:"v4", auth });

function todayStr(){
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: TZ }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
}
function numOr(v,d){ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; }

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
  await sheets.spreadsheets.values.clear({ spreadsheetId:SPREADSHEET_ID, range:`'${sheetTitle}'!A:Z` });
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { requests: [
      { repeatCell: { range:{ sheetId, startRowIndex:0, endRowIndex:2000, startColumnIndex:0, endColumnIndex:26 }, cell:{ userEnteredFormat:{} }, fields:"userEnteredFormat" } },
      { updateBorders: { range:{ sheetId, startRowIndex:0, endRowIndex:2000, startColumnIndex:0, endColumnIndex:26 },
        top:{style:"NONE"}, bottom:{style:"NONE"}, left:{style:"NONE"}, right:{style:"NONE"},
        innerHorizontal:{style:"NONE"}, innerVertical:{style:"NONE"} } }
    ]}
  });
}

async function fetchVCMapDOM(){
  const { chromium } = await import("playwright");
  const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
  const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
  const pg  = await ctx.newPage();
  await pg.goto(VC_URL, { waitUntil: 'domcontentloaded' });
  await pg.waitForSelector('.container .out-row .name', { timeout: 20000 }).catch(()=>{});
  await pg.waitForLoadState('networkidle').catch(()=>{});
  await pg.waitForTimeout(1000);

  const recs = await pg.evaluate((targets)=>{
    const out = {};
    const toNum = s => { const x=parseFloat(String(s||"").replace(/,/g,"").trim()); return Number.isFinite(x)?x:null; };
    const pct2d = s => { const m=String(s||"").match(/(-?\d+(?:\.\d+)?)\s*%/); if(!m) return null; const v=parseFloat(m[1])/100; return v };

    const rows = Array.from(document.querySelectorAll('.container .row'));
    const nameDivs = Array.from(document.querySelectorAll('.container .out-row .name'));

    if (rows.length === 0 || nameDivs.length === 0 || rows.length !== nameDivs.length) {
        return { error: 'Could not find matching data rows and name divs.' };
    }

    for (const [code, target] of Object.entries(targets)) {
        let targetIndex = -1;
        for (let i = 0; i < nameDivs.length; i++) {
            const nameDivText = nameDivs[i].textContent || '';
                       if (nameDivText.includes(target.name) || nameDivText.includes(target.code)) {
                targetIndex = i;
                break;
            }
        }
        if (targetIndex !== -1) {
            const dataRow = rows[targetIndex];
            if (dataRow) {
                const peEl = dataRow.querySelector('.pe');
                const roeEl = dataRow.querySelector('.roe');
                const pe = toNum(peEl ? peEl.textContent : null);
                const roe = pct2d(roeEl ? roeEl.textContent : null);
                if(pe && pe > 0) out[code] = { pe, roe };
            }
        }
    }
    return out;
  }, VC_TARGETS);

  await br.close();
  dbg("VC map (DOM)", recs);
  return recs || {};
}

let VC_CACHE = null;
async function getVC(code){
  if(!VC_CACHE){
    try { VC_CACHE = await fetchVCMapDOM(); }
    catch(e){ dbg("VC DOM err", e.message); VC_CACHE = {}; }
  }
  return VC_CACHE[code] || null;
}

// ===== r_f / ERP* =====
async function rfCN(){ try{
  const url="https://cn.investing.com/rates-bonds/china-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const plain=h.replace(/<[^>]+>/g," "); const near=plain.match(/(\d{1,2}\.\d{1,4})\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","CN 10Y")` };
  }}catch{} return { v:RF_CN, tag:"兜底", link:"—" }; }
async function rfUS(){ try{
  const url="https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const plain=h.replace(/<[^>]+>/g," "); const near=plain.match(/(\d{1,2}\.\d{1,4})\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","US 10Y")` };
  }}catch{} return { v:RF_US, tag:"兜底", link:"—" }; }
async function rfJP(){ try{
  const url="https://cn.investing.com/rates-bonds/japan-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const plain=h.replace(/<[^>]+>/g," "); const near=plain.match(/(\d{1,2}\.\d{1,4})\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","JP 10Y")` };
  }}catch{} return { v:RF_JP, tag:"兜底", link:"—" }; }
async function rfDE(){ try{
  const url="https://www.investing.com/rates-bonds/germany-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const plain=h.replace(/<[^>]+>/g," "); const near=plain.match(/(\d{1,2}\.\d{1,4})\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","DE 10Y")` };
  }}catch{} return { v:RF_DE, tag:"兜底", link:"—" }; }
async function rfIN(){ try{
  const url="https://cn.investing.com/rates-bonds/india-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const plain=h.replace(/<[^>]+>/g," "); const near=plain.match(/(\d{1,2}\.\d{1,4})\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","IN 10Y")` };
  }}catch{} return { v:RF_IN, tag:"兜底", link:"—" }; }

async function erpFromDamodaran(re){
  try{
    const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
    const r = await fetch(url, { headers:{ "User-Agent": UA }, timeout: 15000 });
    if(r.ok){
      const h = await r.text();
      const rows = h.split("</tr>");
      const row  = rows.find(x => re.test(x)) || "";
      const plain = row.replace(/<[^>]+>/g," ");
      const nums = [...plain.matchAll(/(\d{1,2}\.\d{1,2})\s*%/g)].map(m=>Number(m[1]));
      const v = nums.find(x=>x>2 && x<10);
      if(v!=null) return { v:v/100, tag:"真实", link:`=HYPERLINK("${url}","Damodaran")` };
    }
  }catch{}
  return null;
}
async function erpCN(){ return (await erpFromDamodaran(/China/i)) || { v:0.0527, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }
async function erpUS(){ return (await erpFromDamodaran(/(United\s*States|USA)/i)) || { v:0.0433, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }
async function erpJP(){ return (await erpFromDamodaran(/Japan/i)) || { v:0.0527, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }
async function erpDE(){ return (await erpFromDamodaran(/Germany/i)) || { v:0.0433, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }
async function erpIN(){ return (await erpFromDamodaran(/India/i)) || { v:0.0726, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }

// ===== Nikkei：PE & PB =====
async function peNikkei(){
  const { chromium } = await import("playwright");
  const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
  const ctx = await br.newContext({ userAgent: UA, locale: 'en-US', timezoneId: TZ });
  const pg  = await ctx.newPage();
  const url = "https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per";
  await pg.goto(url, { waitUntil: 'domcontentloaded' });
  await pg.waitForSelector("table", { timeout: 8000 }).catch(()=>{});
  await pg.waitForTimeout(600);
  const val = await pg.evaluate(()=>{
    const tbl = document.querySelector("table"); if(!tbl) return null;
    const rows = Array.from(tbl.querySelectorAll("tbody tr"));
    const row = rows[rows.length-1]; if(!row) return null;
    const tds = Array.from(row.querySelectorAll("td"));
    if(tds.length<3) return null;
    const txt = (tds[2].innerText||"").replace(/,/g,"").trim();
    const n = parseFloat(txt); return Number.isFinite(n)? n : null;
  });
  await br.close();
  if(Number.isFinite(val) && val>0 && val<1000) return { v:val, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PER")` };
}
async function pbNikkei(){
  const { chromium } = await import("playwright");
  const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
  const ctx = await br.newContext({ userAgent: UA, locale: 'en-US', timezoneId: TZ });
  const pg  = await ctx.newPage();
  const url = "https://indexes.nikkei.co.jp/en/nkave/archives/data?list=pbr";
  await pg.goto(url, { waitUntil: 'domcontentloaded' });
  await pg.waitForSelector("table", { timeout: 8000 }).catch(()=>{});
  await pg.waitForTimeout(600);
  const val = await pg.evaluate(()=>{
    const tbl = document.querySelector("table"); if(!tbl) return null;
    const rows = Array.from(tbl.querySelectorAll("tbody tr"));
    const row = rows[rows.length-1]; if(!row) return null;
    const tds = Array.from(row.querySelectorAll("td"));
    if(tds.length<3) return null;
    const txt = (tds[2].innerText||"").replace(/,/g,"").trim();
    const n = parseFloat(txt); return Number.isFinite(n)? n : null;
  });
  await br.close();
  if(Number.isFinite(val) && val>0 && val<1000) return { v:val, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PBR")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PBR")` };
}

// ===== Nifty 50 =====
async function fetchNifty50(){
  const { chromium } = await import("playwright");
  const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
  const ctx = await br.newContext({ userAgent: UA, locale: 'en-US', timezoneId: TZ });
  const pg  = await ctx.newPage();
  const url = "https://trendlyne.com/equity/PE/NIFTY/1887/nifty-50-price-to-earning-ratios/";
  try {
    await pg.goto(url, { waitUntil: 'networkidle', timeout: 25000 });
    await pg.waitForTimeout(2000);
    const values = await pg.evaluate(() => {
      let pe = null, pb = null;
      const peTitle = document.querySelector('title');
      if (peTitle) {
        const m = peTitle.textContent.match(/of NIFTY is ([\d\.]+)/);
        if (m && m[1]) pe = parseFloat(m[1]);
      }
      const allRows = Array.from(document.querySelectorAll('tr.stock-indicator-tile-v2'));
      const pbRow = allRows.find(row => {
        const t = row.querySelector('th a span.stock-indicator-title');
        return t && t.textContent.includes('PB');
      });
      if (pbRow) {
        const v = pbRow.querySelector('td.block_content span.fs1p5rem');
        if (v) pb = parseFloat(v.textContent.trim());
      }
      return { pe, pb };
    });
    const peRes = (Number.isFinite(values.pe) && values.pe > 0) ? { v: values.pe, tag: "真实", link: `=HYPERLINK("${url}","Nifty PE")` } : { v: "", tag: "兜底", link: `=HYPERLINK("${url}","Nifty PE")` };
    const pbRes = (Number.isFinite(values.pb) && values.pb > 0) ? { v: values.pb, tag: "真实", link: `=HYPERLINK("${url}","Nifty PB")` } : { v: "", tag: "兜底", link: `=HYPERLINK("${url}","Nifty PB")` };
    return { peRes, pbRes };
  } finally {
    await br.close();
  }
}

// ===== Tencent via Google (r.jina.ai text) =====
async function fetchTencentData() {
  const toNum = v => (v!=null && Number.isFinite(Number(v))? Number(v): null);

  const parseAbbrev = s => {
    if(!s) return null;
    const u = String(s).replace(/,/g,"").trim().toUpperCase();
    const m = u.match(/([\d.]+)\s*([KMBT]?)/);
    if(!m) return null;
    const n = parseFloat(m[1]);
    if(!Number.isFinite(n)) return null;
    const unit = (m[2]||"").toUpperCase();
    const mul = unit==="T"?1e12: unit==="B"?1e9: unit==="M"?1e6: unit==="K"?1e3: 1;
    return n*mul;
  };

  // ENV overrides as final fallback
  const envMc = toNum(process.env.TENCENT_MC_OVERRIDE);
  const envSh = toNum(process.env.TENCENT_SHARES_OVERRIDE);

  let marketCap = null;   // 港元
  let totalShares = null; // 股

  // 1) Google Finance text (r.jina.ai)
  try {
    const url = "https://r.jina.ai/http://www.google.com/finance/quote/0700:HK?hl=en";
    const r = await fetch(url, { headers: { "User-Agent": UA }, timeout: 20000 });
    if (r.ok) {
      const txt = await r.text();

      // Market cap
      let mc = null;
      let m1 = txt.match(/Market\s*cap[^\n]*?(HK\$|HKD)\s*([\d.,]+)\s*([TMBK]?)/i);
      if (m1) {
        const n = parseFloat(String(m1[2]).replace(/,/g,""));
        if (Number.isFinite(n)) {
          const unit = (m1[3]||"").toUpperCase();
          const mul = unit==="T"?1e12: unit==="B"?1e9: unit==="M"?1e6: unit==="K"?1e3: 1;
          mc = n*mul;
        }
      }
      if (!mc) {
        const m2 = txt.match(/([\d.]+)\s*(trillion|billion|million)\s*(HKD|HK\$)/i);
        if (m2) {
          const n = parseFloat(m2[1]);
          const unit = m2[2].toLowerCase();
          const mul = unit==="trillion"?1e12: unit==="billion"?1e9: unit==="million"?1e6: 1;
          if (Number.isFinite(n)) mc = n*mul;
        }
      }
      marketCap = mc || marketCap;

      // Shares outstanding
      const sh = txt.match(/Shares\s*outstanding[^\n]*?([\d.,]+)\s*([KMBT]?)/i);
      if (sh) {
        const n = parseFloat(String(sh[1]).replace(/,/g,""));
        if (Number.isFinite(n)) {
          const unit = (sh[2]||"").toUpperCase();
          const mul = unit==="T"?1e12: unit==="B"?1e9: unit==="M"?1e6: unit==="K"?1e3: 1;
          totalShares = n*mul;
        }
      }
    }
  } catch(e) { dbg("Tencent(GF r.jina.ai) err", e.message); }

  // 2) Google SERP text fallback (market cap)
  if (!marketCap) {
    try {
      const url = "https://r.jina.ai/http://www.google.com/search?q=0700.HK+market+cap&hl=en";
      const r = await fetch(url, { headers: { "User-Agent": UA }, timeout: 20000 });
      if (r.ok) {
        const txt = await r.text();
        let mc = null;
        const m2 = txt.match(/([\d.]+)\s*(trillion|billion|million)\s*(HKD|HK\$)/i);
        if (m2) {
          const n = parseFloat(m2[1]);
          const unit = m2[2].toLowerCase();
          const mul = unit==="trillion"?1e12: unit==="billion"?1e9: unit==="million"?1e6: 1;
          if (Number.isFinite(n)) mc = n*mul;
        }
        if (!mc) {
          const m3 = txt.match(/HK\$?\s*([\d.]+)\s*([TMBK])/i);
          if (m3) {
            const n = parseFloat(m3[1]);
            const unit = (m3[2]||"").toUpperCase();
            const mul = unit==="T"?1e12: unit==="B"?1e9: unit==="M"?1e6: unit==="K"?1e3: 1;
            if (Number.isFinite(n)) mc = n*mul;
          }
        }
        marketCap = mc || marketCap;
      }
    } catch(e) { dbg("Tencent(SERP r.jina.ai) err", e.message); }
  }

  dbg("Tencent(r.jina.ai) mc/sh", marketCap, totalShares);

  // 3) ENV fallback
  marketCap   = marketCap   || envMc || null;
  totalShares = totalShares || envSh || null;

  return { marketCap, totalShares };
}

// ===== Moutai price via Google Finance (r.jina.ai text) =====
async function fetchMoutaiPrice(){
  try{
    const url = "https://r.jina.ai/http://www.google.com/finance/quote/SHA:600519?hl=zh-CN";
    const r = await fetch(url, { headers:{ "User-Agent": UA }, timeout: 20000 });
    if (r.ok){
      const txt = await r.text();
      // 形态：¥1,481.61 或 CNY 1481.61
      const m = txt.match(/(?:¥|CNY)\s*([\d,]+(?:\.\d+)?)/);
      if (m){
        const price = Number(m[1].replace(/,/g,""));
        if (Number.isFinite(price)) {
          return { price, tag:"真实", link:'=HYPERLINK("https://www.google.com/finance/quote/SHA:600519","Google 财经")' };
        }
      }
    }
  }catch(e){ dbg("Moutai(r.jina.ai) err", e.message); }
  return { price: null, tag:"兜底", link:'=HYPERLINK("https://www.google.com/finance/quote/SHA:600519","Google 财经")' };
}

// ===== 写块 & 判定（指数） =====
async function writeBlock(startRow,label,country,peRes,rfRes,erpStar,erpTag,erpLink,roeRes){
  const { sheetTitle, sheetId } = await ensureToday();
  const pe = (peRes?.
