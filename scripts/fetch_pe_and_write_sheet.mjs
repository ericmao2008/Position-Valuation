/**
 * Version History
 * V2.9.6 - Nikkei ROE Calculation
 * - Implemented the proposed logic to calculate Nikkei ROE from official PE and PB values.
 * - Added a new `pbNikkei` function to scrape the PBR value from indexes.nikkei.co.jp.
 * - The Main block now fetches both PE and PB for Nikkei and calculates ROE = PB / PE.
 * - Removed the manual `ROE_JP` environment variable override for Nikkei.
 *
 * V2.9.5 - Feature Expansion: Added NDX, DAX, India Indices
 * - Added Nasdaq 100, Germany DAX, and MSCI India to the VC_TARGETS.
 * - Created new functions (rfDE, rfIN, erpDE, erpIN) to fetch bond yields and ERPs for Germany and India.
 * - Updated the Main block to process, write, and report on all 8 indices.
 * - Refined `writeBlock` to handle labels for different countries.
 * - Added new PE_OVERRIDE environment variables for the new indices.
 *
 * V2.9.4 - UI Polish
 * - Modified the '判定' (Judgment) field in `writeBlock` to only show the emoji (🟢, 🔴, 🟡) 
 * and remove the descriptive text, as requested. This affects both the sheet and email summary.
 *
 * V2.9.3 - Feature Removal
 * - Removed the "新经济" (New Economy) index from all processing sections as requested.
 * - Cleaned up related constants and logic in the Main function.
 * * V2.9.2 - Final Formatting & Feature Polish
 * - Fixed off-by-one error in format application logic within `writeBlock`.
 * - ROE rows are now correctly formatted as percentages (0.00%).
 * - ROE Factor row is now correctly formatted as a decimal (0.00).
 * - Added the "新经济" index to the main processing loop to ensure it's written to the sheet.
 * - Enhanced email summary to include ROE values for a more complete overview.
 * * V2.9.1 - The Great Refactor (Complete File)
 * - Final complete version by Gemini, adhering to the principle of providing full files only.
 * - Rewrote the core scraping logic in `fetchVCMapDOM` to adapt to the new div-based layout on danjuanfunds.com.
 * - Logic now locates data by finding specific class name prefixes (e.g., "pe___", "roe___"), which is more robust.
 * - Full logic for all 5 target indices is present in the Main function.
 *
 * V2.7.4
 *  - 统一改为 “表格解析” 的 Value Center 抓取（仅 HS300/SP500/CSIH30533/HSTECH）：
 *      * 通过 <a href="/dj-valuation-table-detail/<CODE>"> 锁定对应 <tr>
 *      * 第 2 列取 PE（小数）；第 7 列取 ROE（百分比 → 小数）
 *      * HTTP 优先，如需再 Playwright 打开同页读取 page.content() 再解析
 *  - 口径：HS300/CSIH30533/HSTECH → r_f=中国10Y，ERP*=China；SP500 → r_f=US10Y，ERP*=US
 *  - Nikkei 仍用官方档案页 PER；ROE 暂用 ROE_JP（小数）可覆写
 *  - 判定：基于 P/E 与 [买点, 卖点] 区间；邮件正文包含判定；DEBUG 保留
 *
 * V2.7.3
 *  - 修复：重复 import nodemailer
 *
 * V2.7.2
 *  - 修复 peNikkei 未定义；Value Center-only（除 Nikkei）；HSTECH 与中概口径一致
 *
 * V2.7.1
 *  - 修复 roeFromDanjuan 未定义；保留 Value Center 优先、邮件判定、恒生科技分块
 *
 * V2.7.0-test
 *  - 新增恒生科技（HSTECH）；Value Center 优先抓取；邮件正文加入判定
 *
 * V2.6.11
 *  - 修复：P/E 抓取函数占位导致 undefined；恢复并加固四个 pe 函数
 *
 * V2.6.10
 *  - 修复：CSIH30533 的 ROE(TTM) 丢失（点击 ROE tab + JSON 优先 + 3%~40% 过滤）
 *  - 邮件：支持 MAIL_FROM_EMAIL/MAIL_FROM_NAME；text+html；verify + DEBUG
 *
 * V2.6.9
 *  - 判定：基于 P/E 与 [买点, 卖点] 区间；内建邮件 DEBUG（verify/send/FORCE_EMAIL）
 *
 * V2.6.8
 *  - 修复：中概 ROE 偶发抓成 30%（更严格匹配与范围过滤）
 *
 * V2.6.7
 *  - 去除“中枢（对应P/E上限）”；仅保留买点/卖点/合理区间；公式写入说明
 *
 * V2.6.6
 *  - 指数行高亮；去表头行；ROE 百分比、因子小数；版本日志保留
 *
 * V2.6.5
 *  - 清空当日 Sheet（值+样式+边框）；统一 totalRows；每块后留 1 空行
 *
 * V2.6.4
 *  - 修复写入范围与实际行数不一致
 *
 * V2.6.3
 *  - 方案B：加入“合理PE（ROE因子）”；在说明中写明公式
 *
 * V2.6.2
 *  - 去除多余 P/E 行；每块加粗浅灰与外框；曾并行显示“原始阈值/ROE因子阈值”
 *
 * V2.6.1 (hotfix)
 *  - 百分比格式修正；ROE(TTM) 抓取增强（Playwright/HTTP）
 *
 * V2.6
 *  - 引入 ROE 因子：PE_limit = 1/(r_f+ERP*) × (ROE/ROE_BASE)
 *
 * V2.5
 *  - CSIH30533 切中国口径：r_f=中国10Y，ERP*=China
 *
 * V2.4
 *  - 新增 CSIH30533 分块；多路兜底
 *
 * V2.3
 *  - δ → P/E 空间三阈值
 *
 * V2.2
 *  - Nikkei 修复；空串不写 0
 *
 * V2.1
 *  - 新增 Nikkei 225
 *
 * V2.0
 *  - HS300 + SPX 基础版
 */

import fetch from "node-fetch";
import { google } from "googleapis";
import nodemailer from "nodemailer";
import fs from "fs";

// ===== Global =====
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PW = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";
const DEBUG  = String(process.env.DEBUG_VERBOSE ?? "0") === "1";
const TZ     = process.env.TZ || "Asia/Shanghai";
const dbg    = (...a)=>{ if(DEBUG) console.log("[DEBUG]", ...a); };

const VC_URL = "https://danjuanfunds.com/djmodule/value-center?channel=1300100141";

// 目标指数
const VC_TARGETS = {
  SH000300: { name: "沪深300", code: "SH000300", country: "CN" },
  SP500:    { name: "标普500", code: "SP500", country: "US" },
  CSIH30533:{ name: "中概互联50", code: "CSIH30533", country: "CN" },
  HSTECH:   { name: "恒生科技", code: "HKHSTECH", country: "CN" },
  NDX:      { name: "纳指100", code: "NDX", country: "US" },
  GDAXI:    { name: "德国DAX", code: "GDAXI", country: "DE" },
  "935600": { name: "MSCI印度", code: "935600", country: "IN" },
};

// ===== Policy / Defaults =====
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.005);
const ROE_BASE      = numOr(process.env.ROE_BASE,   0.12);

const RF_CN = numOr(process.env.RF_CN, 0.023); // 兜底-中国
const RF_US = numOr(process.env.RF_US, 0.0425); // 兜底-美国
const RF_JP = numOr(process.env.RF_JP, 0.0100); // 兜底-日本
const RF_DE = numOr(process.env.RF_DE, 0.025); // 兜底-德国
const RF_IN = numOr(process.env.RF_IN, 0.07);  // 兜底-印度

const PE_OVERRIDE_CN      = (()=>{ const s=(process.env.PE_OVERRIDE_CN??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_SPX     = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_CXIN    = (()=>{ const s=(process.env.PE_OVERRIDE_CXIN??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_HSTECH  = (()=>{ const s=(process.env.PE_OVERRIDE_HSTECH??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_NDX     = (()=>{ const s=(process.env.PE_OVERRIDE_NDX??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_DAX     = (()=>{ const s=(process.env.PE_OVERRIDE_DAX??"").trim(); return s?Number(s):null; })();
const PE_OVERRIDE_IN      = (()=>{ const s=(process.env.PE_OVERRIDE_IN??"").trim(); return s?Number(s):null; })();

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
  const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
  const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
  const pg  = await ctx.newPage();
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
      const row  = rows.find(x => re.test(x)) || "";
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

// ===== Nikkei：PE & PB (DOM-only) =====
async function peNikkei(){
  const { chromium } = await import("playwright");
  const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
  const ctx = await br.newContext({ userAgent: UA, locale: 'en-US', timezoneId: TZ });
  const pg  = await ctx.newPage();
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
  const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
  const ctx = await br.newContext({ userAgent: UA, locale: 'en-US', timezoneId: TZ });
  const pg  = await ctx.newPage();
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

// ===== 写块 & 判定 =====
async function writeBlock(startRow,label,country,peRes,rfRes,erpStar,erpTag,erpLink,roeRes){
  const { sheetTitle, sheetId } = await ensureToday();
  const pe = (peRes?.v==="" || peRes?.v==null) ? null : Number(peRes?.v);
  const rf = Number.isFinite(rfRes?.v) ? rfRes.v : null;
  const roe = Number.isFinite(roeRes?.v) ? roeRes.v : null;
  const ep = Number.isFinite(pe) ? 1/pe : null;
  const factor = (roe!=null && roe>0) ? (roe/ROE_BASE) : 1;
  const factorDisp = (roe!=null && roe>0) ? Number(factor.toFixed(2)) : "";
  const peBuy  = (rf!=null && erpStar!=null) ? Number((1/(rf+erpStar+DELTA)*factor).toFixed(2)) : null;
  const peSell = (rf!=null && erpStar!=null && (rf+erpStar-DELTA)>0) ? Number((1/(rf+erpStar-DELTA)*factor).toFixed(2)) : null;
  const fairRange = (peBuy!=null && peSell!=null) ? `${peBuy} ~ ${peSell}` : "";
  let status="需手动更新";
  if(Number.isFinite(pe) && peBuy!=null && peSell!=null){
    if (pe <= peBuy) status="🟢";
    else if (pe >= peSell) status="🔴";
    else status="🟡";
  }
  const rfLabel = `${country} 10Y`;
  const rows = [
    ["指数", label, "真实", "宽基/行业指数估值分块", peRes?.link || "—"],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peRes?.tag || (Number.isFinite(pe)?"真实":"兜底"), "估值来源", peRes?.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rf!=null?"真实":"兜底", rfLabel, rfRes?.link || "—"],
    ["目标 ERP*", (Number.isFinite(erpStar)?erpStar:""), (Number.isFinite(erpStar)?"真实":"兜底"), "达摩达兰", erpLink || '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换（说明用，不定义卖点）","—"],
    ["买点PE上限（含ROE因子）", peBuy ?? "", (peBuy!=null)?"真实":"兜底", "买点=1/(r_f+ERP*+δ)×factor","—"],
    ["卖点PE下限（含ROE因子）", peSell ?? "", (peSell!=null)?"真实":"兜底", "卖点=1/(r_f+ERP*−δ)×factor","—"],
    ["合理PE区间（含ROE因子）", fairRange, (peBuy!=null && peSell!=null)?"真实":"兜底", "买点上限 ~ 卖点下限","—"],
    ["ROE（TTM）", roe ?? "", roeRes?.tag || "—", "盈利能力（小数，显示为百分比）", roeRes?.link || "—"],
    ["ROE基准（可配 env.ROE_BASE）", ROE_BASE, "真实", "默认 0.12 = 12%","—"],
    ["ROE倍数因子 = ROE/ROE基准", factorDisp, (factorDisp!=="")?"真实":"兜底", "例如 16.4%/12% = 1.36","—"],
    ["说明（公式）", "见右", "真实", "买点=1/(r_f+ERP*+δ)×factor；卖点=1/(r_f+ERP*−δ)×factor；合理区间=买点~卖点","—"],
    ["判定", status, (Number.isFinite(pe) && peBuy!=null && peSell!=null)?"真实":"兜底", "基于 P/E 与区间","—"],
  ];
  const end = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`, rows);
  const requests = [];
  [2,3,4,5,9,10].forEach(i=>{ const r=(startRow-1)+i;
    requests.push({ repeatCell:{ range:{ sheetId, startRowIndex:r, endRowIndex:r+1, startColumnIndex:1, endColumnIndex:2 },
      cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } }, fields:"userEnteredFormat.numberFormat" }}); });
  [1,6,7,11].forEach(i=>{ const r=(startRow-1)+i;
    requests.push({ repeatCell:{ range:{ sheetId, startRowIndex:r, endRowIndex:r+1, startColumnIndex:1, endColumnIndex:2 },
      cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" }}); });
  requests.push({ repeatCell:{ range:{ sheetId, startRowIndex:(startRow-1)+0, endRowIndex:(startRow-1)+1, startColumnIndex:0, endColumnIndex:5 },
    cell:{ userEnteredFormat:{ backgroundColor:{ red:0.95, green:0.95, blue:0.95 }, textFormat:{ bold:true } } }, fields:"userEnteredFormat(backgroundColor,textFormat)" }});
  requests.push({ updateBorders:{ range:{ sheetId, startRowIndex:(startRow-1), endRowIndex:end, startColumnIndex:0, endColumnIndex:5 },
    top:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
    bottom:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
    left:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
    right:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } } }});
  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests } });

  return { nextRow: end + 2, judgment: status, pe, roe };
}

// ===== Email =====
async function sendEmailIfEnabled(lines){
  const { SMTP_HOST,SMTP_PORT,SMTP_USER,SMTP_PASS,MAIL_TO,MAIL_FROM_NAME,MAIL_FROM_EMAIL,FORCE_EMAIL } = process.env;
  if(!SMTP_HOST||!SMTP_PORT||!SMTP_USER||!SMTP_PASS||!MAIL_TO){ dbg("[MAIL] skip env"); return; }
  const transporter = nodemailer.createTransport({ host:SMTP_HOST, port:Number(SMTP_PORT), secure:Number(SMTP_PORT)===465, auth:{ user:SMTP_USER, pass:SMTP_PASS }});
  try{ dbg("[MAIL] verify start",{host:SMTP_HOST,user:SMTP_USER,to:MAIL_TO}); await transporter.verify(); dbg("[MAIL] verify ok"); }
  catch(e){ console.error("[MAIL] verify fail:",e); if(!FORCE_EMAIL) return; console.error("[MAIL] FORCE_EMAIL=1, continue"); }
  const fromEmail = MAIL_FROM_EMAIL || SMTP_USER;
  const from = MAIL_FROM_NAME ? `${MAIL_FROM_NAME} <${fromEmail}>` : fromEmail;
  const subject = `Valuation Daily — ${todayStr()} (${TZ})`;
  const text = [`Valuation Daily — ${todayStr()} (${TZ})`, ...lines.map(s=>`• ${s}`), ``, `See sheet "${todayStr()}" for thresholds & judgments.`].join('\n');
  const html = [`<h3>Valuation Daily — ${todayStr()} (${TZ})`, `<ul>${lines.map(s=>`<li>${s}</li>`).join("")}</ul>`, `<p>See sheet "${todayStr()}" for thresholds & judgments.</p>`].join("");
  dbg("[MAIL] send start",{subject,to:MAIL_TO,from});
  try{ const info = await transporter.sendMail({ from, to:MAIL_TO, subject, text, html }); console.log("[MAIL] sent",{ messageId: info.messageId, response: info.response }); }
  catch(e){ console.error("[MAIL] send error:", e); }
}

// ===== Main =====
(async()=>{
  console.log("[INFO] Run start", todayStr(), "USE_PLAYWRIGHT=", USE_PW, "TZ=", TZ);

  let row=1;
  const { sheetTitle, sheetId } = await ensureToday();
  await clearTodaySheet(sheetTitle, sheetId);

  let vcMap = {};
  if (USE_PW) {
    try { vcMap = await fetchVCMapDOM(); } catch(e){ dbg("VC DOM err", e.message); vcMap = {}; }
    
    if (Object.keys(vcMap).length < Object.keys(VC_TARGETS).length && USE_PW) {
      console.error("[ERROR] Scraping from Value Center was incomplete. Exiting with error code 1 to trigger artifact upload.");
      process.exit(1);
    }
  }

  const rf_cn_promise = rfCN();
  const erp_cn_promise = erpCN();
  const rf_us_promise = rfUS();
  const erp_us_promise = erpUS();
  const pe_nk_promise = peNikkei();
  const pb_nk_promise = pbNikkei();
  const rf_jp_promise = rfJP();
  const erp_jp_promise = erpJP();
  const rf_de_promise = rfDE();
  const erp_de_promise = erpDE();
  const rf_in_promise = rfIN();
  const erp_in_promise = erpIN();

  // 1) HS300
  let r_hs = vcMap["SH000300"];
  let pe_hs = r_hs?.pe ? { v: r_hs.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_CN??"", tag:"兜底", link:"—" };
  let roe_hs = r_hs?.roe ? { v: r_hs.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  let res_hs = await writeBlock(row, VC_TARGETS.SH000300.name, "CN", pe_hs, await rf_cn_promise, (await erp_cn_promise).v, "真实", null, roe_hs);
  row = res_hs.nextRow;

  // 2) SP500
  let r_sp = vcMap["SP500"];
  let pe_spx = r_sp?.pe ? { v: r_sp.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_SPX??"", tag:"兜底", link:"—" };
  let roe_spx = r_sp?.roe ? { v: r_sp.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  const erp_us = await erp_us_promise;
  let res_sp = await writeBlock(row, VC_TARGETS.SP500.name, "US", pe_spx, await rf_us_promise, erp_us.v, erp_us.tag, erp_us.link, roe_spx);
  row = res_sp.nextRow;
  
  // 3) 纳指100
  let r_ndx = vcMap["NDX"];
  let pe_ndx = r_ndx?.pe ? { v: r_ndx.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_NDX??"", tag:"兜底", link:"—" };
  let roe_ndx = r_ndx?.roe ? { v: r_ndx.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  let res_ndx = await writeBlock(row, VC_TARGETS.NDX.name, "US", pe_ndx, await rf_us_promise, erp_us.v, erp_us.tag, erp_us.link, roe_ndx);
  row = res_ndx.nextRow;

  // 4) Nikkei
  const pe_nk = await pe_nk_promise;
  const pb_nk = await pb_nk_promise;
  let roe_nk = { v: null, tag: "计算值", link: pe_nk.link };
  if (pe_nk && pe_nk.v && pb_nk && pb_nk.v) { roe_nk.v = pb_nk.v / pe_nk.v; }
  const erp_jp = await erp_jp_promise;
  let res_nk = await writeBlock(row, "日经指数", "JP", pe_nk, await rf_jp_promise, erp_jp.v, erp_jp.tag, erp_jp.link, roe_nk);
  row = res_nk.nextRow;

  // 5) 中概互联50
  let r_cx = vcMap["CSIH30533"];
  let pe_cx = r_cx?.pe ? { v: r_cx.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_CXIN??"", tag:"兜底", link:"—" };
  let roe_cx = r_cx?.roe ? { v: r_cx.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  const erp_cn = await erp_cn_promise;
  let res_cx = await writeBlock(row, VC_TARGETS.CSIH30533.name, "CN", pe_cx, await rf_cn_promise, erp_cn.v, erp_cn.tag, erp_cn.link, roe_cx);
  row = res_cx.nextRow;

  // 6) 恒生科技
  let r_hst = vcMap["HSTECH"];
  let pe_hst = r_hst?.pe ? { v: r_hst.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_HSTECH??"", tag:"兜底", link:"—" };
  let roe_hst = r_hst?.roe ? { v: r_hst.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  let res_hst = await writeBlock(row, VC_TARGETS.HSTECH.name, "CN", pe_hst, await rf_cn_promise, erp_cn.v, erp_cn.tag, erp_cn.link, roe_hst);
  row = res_hst.nextRow;

  // 7) 德国DAX
  let r_dax = vcMap["GDAXI"];
  let pe_dax = r_dax?.pe ? { v: r_dax.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_DAX??"", tag:"兜底", link:"—" };
  let roe_dax = r_dax?.roe ? { v: r_dax.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  const erp_de = await erp_de_promise;
  let res_dax = await writeBlock(row, VC_TARGETS.GDAXI.name, "DE", pe_dax, await rf_de_promise, erp_de.v, erp_de.tag, erp_de.link, roe_dax);
  row = res_dax.nextRow;

  // 8) MSCI印度
  let r_in = vcMap["935600"];
  let pe_in = r_in?.pe ? { v: r_in.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_IN??"", tag:"兜底", link:"—" };
  let roe_in = r_in?.roe ? { v: r_in.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  const erp_in = await erp_in_promise;
  let res_in = await writeBlock(row, VC_TARGETS["935600"].name, "IN", pe_in, await rf_in_promise, erp_in.v, erp_in.tag, erp_in.link, roe_in);
  row = res_in.nextRow;
  
  console.log("[DONE]", todayStr(), {
    hs300_pe: res_hs.pe, spx_pe: res_sp.pe, ndx_pe: res_ndx.pe, nikkei_pe: res_nk.pe, 
    cxin_pe: res_cx.pe, hstech_pe: res_hst.pe, dax_pe: res_dax.pe, in_pe: res_in.pe
  });
  
  const roeFmt = (r) => r != null ? ` (ROE: ${(r * 100).toFixed(2)}%)` : '';

  const lines = [
    `HS300 PE: ${res_hs.pe ?? "-"} ${roeFmt(res_hs.roe)}→ ${res_hs.judgment ?? "-"}`,
    `SPX PE: ${res_sp.pe ?? "-"} ${roeFmt(res_sp.roe)}→ ${res_sp.judgment ?? "-"}`,
    `NDX PE: ${res_ndx.pe ?? "-"} ${roeFmt(res_ndx.roe)}→ ${res_ndx.judgment ?? "-"}`,
    `Nikkei PE: ${res_nk.pe ?? "-"} ${roeFmt(res_nk.roe)}→ ${res_nk.judgment ?? "-"}`,
    `China Internet PE: ${res_cx.pe ?? "-"} ${roeFmt(res_cx.roe)}→ ${res_cx.judgment ?? "-"}`,
    `HSTECH PE: ${res_hst.pe ?? "-"} ${roeFmt(res_hst.roe)}→ ${res_hst.judgment ?? "-"}`,
    `DAX PE: ${res_dax.pe ?? "-"} ${roeFmt(res_dax.roe)}→ ${res_dax.judgment ?? "-"}`,
    `MSCI India PE: ${res_in.pe ?? "-"} ${roeFmt(res_in.roe)}→ ${res_in.judgment ?? "-"}`
  ];
  await sendEmailIfEnabled(lines);
})();
