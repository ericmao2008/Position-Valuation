/**
 * Version History
 * V2.7.1
 *  - 修复：测试版调用 roeFromDanjuan 未定义导致 ReferenceError；补回并加固该函数
 *  - 保持：Value Center 聚合页优先抓取（HS300/SP500/CSIH30533/HSTECH 的 PE/ROE）
 *  - 邮件：正文包含判定（🟢/🟡/🔴），保留 verify/send DEBUG
 *
 * V2.7.0-test
 *  - 新增指数：恒生科技（HSTECH）
 *  - 新增 Value Center 优先抓取；失败回退老抓法
 *  - 邮件正文加入各指数判定
 *
 * V2.6.11
 *  - 修复：P/E 抓取函数误留为占位导致 undefined；恢复并加固四个 pe 函数
 *
 * V2.6.10
 *  - 修复：CSIH30533 的 ROE(TTM) 丢失（点击 ROE tab + JSON 优先 + 3%~40% 过滤）
 *  - 邮件：支持 MAIL_FROM_EMAIL/MAIL_FROM_NAME；text+html；verify + DEBUG
 *
 * V2.6.9
 *  - 判定：基于 P/E 与 [买点, 卖点] 区间；内建邮件 DEBUG（verify/send/ FORCE_EMAIL）
 *
 * V2.6.8
 *  - 修复：中概 ROE 偶发抓成 30%（更严格匹配与范围过滤）
 *
 * V2.6.7
 *  - 去除“中枢（对应P/E上限）”；仅保留买点/卖点/合理区间；公式写入说明行
 *
 * V2.6.6
 *  - 指数行高亮；去表头行；ROE 百分比、因子小数；版本日志保留
 *
 * V2.6.5
 *  - 清空当日 Sheet（值+样式+边框）；统一 totalRows；每块后留 1 空行
 *
 * V2.6.4
 *  - 修复写入范围与实际行数不一致
 *
 * V2.6.3
 *  - 方案B：加入“合理PE（ROE因子）”；在说明中写明公式
 *
 * V2.6.2
 *  - 去除多余 P/E 行；每块加粗浅灰与外框；曾并行显示“原始阈值/ROE因子阈值”
 *
 * V2.6.1 (hotfix)
 *  - 百分比格式修正；ROE(TTM) 抓取增强（Playwright/HTTP）
 *
 * V2.6
 *  - 引入 ROE 因子：PE_limit = 1/(r_f+ERP*) × (ROE/ROE_BASE)
 *
 * V2.5
 *  - CSIH30533 切换中国口径：r_f=中国10Y，ERP*=China
 *
 * V2.4
 *  - 新增 CSIH30533 分块；多路兜底
 *
 * V2.3
 *  - δ → P/E 空间三阈值
 *
 * V2.2
 *  - Nikkei 修复；空串不写 0
 *
 * V2.1
 *  - 新增 Nikkei 225
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

// ---------- 参数 ----------
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.005);
const ROE_BASE      = numOr(process.env.ROE_BASE,   0.12);

// r_f 兜底
const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);
const RF_US = numOr(process.env.RF_US,       0.0425);
const RF_JP = numOr(process.env.RF_JP,       0.0100);
const RF_HK = numOr(process.env.RF_HK,       0.0250);

// 覆写
const PE_OVERRIDE_CN      = (()=>{ const s=(process.env.PE_OVERRIDE??"").trim();           return s?Number(s):null; })();
const PE_OVERRIDE_SPX     = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim();       return s?Number(s):null; })();
const PE_OVERRIDE_NIKKEI  = (()=>{ const s=(process.env.PE_OVERRIDE_NIKKEI??"").trim();    return s?Number(s):null; })();
const PE_OVERRIDE_CXIN    = (()=>{ const s=(process.env.PE_OVERRIDE_CXIN??"").trim();      return s?Number(s):null; })();
const PE_OVERRIDE_HSTECH  = (()=>{ const s=(process.env.PE_OVERRIDE_HSTECH??"").trim();    return s?Number(s):null; })();
const ROE_JP = numOr(process.env.ROE_JP, null);

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

// ---------- r_f 抓取 ----------
async function rfCN(){ try{
  const url="https://cn.investing.com/rates-bonds/china-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){ const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const near=t.match(/(收益率|Yield)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) || t.match(/(\d{1,2}\.\d{1,4})\s*%/); if(near) v=Number(near[2]||near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","CN 10Y (Investing)")` };
  } }catch{} return { v:RF_CN, tag:"兜底", link:"—" }; }
async function rfUS(){ const urls=[ "https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield","https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield" ];
  for(const url of urls){ try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
    if(!r.ok) continue; const h=await r.text(); let v=null;
    const m1=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m1) v=Number(m1[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const m2=t.match(/(Yield|收益率)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) || t.match(/(\d{1,2}\.\d{1,4})\s*%/); if(m2) v=Number(m2[2]||m2[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","US 10Y (Investing)")` };
  }catch{} } return { v:RF_US, tag:"兜底", link:"—" }; }
async function rfJP(){ try{
  const url="https://cn.investing.com/rates-bonds/japan-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){ const h=await r.text(); let v=null; const m1=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m1) v=Number(m1[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const m2=t.match(/(Yield|收益率)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) || t.match(/(\d{1,2}\.\d{1,4})\s*%/); if(m2) v=Number(m2[2]||m2[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","JP 10Y (Investing)")` };
  }
}catch{} return { v:RF_JP, tag:"兜底", link:"—" }; }
async function rfHK(){ try{
  const url="https://www.investing.com/rates-bonds/hong-kong-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){ const h=await r.text(); let v=null; const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const n=t.match(/(\d{1,2}\.\d{1,4})\s*%/); if(n) v=Number(n[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","HK 10Y (Investing)")` };
  }
}catch{} return { v:RF_HK, tag:"兜底", link:"—" }; }

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
  return { v:fallbackPct, tag:"兜底", link:`=HYPERLINK("${url}","Damodaran")` };
}
async function erpUS(){ return erpFromDamodaran("United\\s*States|USA", 0.0433); }
async function erpJP(){ return erpFromDamodaran("^\\s*Japan\\s*$|Japan", 0.0527); }
async function erpCN(){ return erpFromDamodaran("^\\s*China\\s*$|China", 0.0527); }
async function erpHK(){ return erpFromDamodaran("^\\s*China\\s*$|China", 0.0527); } // HSTECH 口径（可按你口径切换）

// ---------- Value Center ----------
let VC_CACHE = null;
async function fetchValueCenterMap(){
  const url = "https://danjuanfunds.com/djmodule/value-center?channel=1300100141";
  try{
    const r = await fetch(url, { headers:{ "User-Agent": UA }, timeout: 15000 });
    if(r.ok){
      const h = await r.text();
      const m = parseValueCenterHTML(h);
      if (Object.keys(m).length) { dbg("ValueCenter via HTTP", m); return m; }
    }
  }catch(e){ dbg("VC HTTP err", e.message); }

  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(url, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(1800);
      const html = await pg.content();
      await br.close();
      const m = parseValueCenterHTML(html);
      if (Object.keys(m).length) { dbg("ValueCenter via PW", m); return m; }
    }catch(e){ dbg("VC PW err", e.message); }
  }
  return {};
}
function parseValueCenterHTML(html){
  const map = {};
  const blob = html.replace(/\s+/g, ' ');
  const re = /"index_code"\s*:\s*"([A-Z0-9]+)".{0,200}?"pe_ttm"\s*:\s*"?([\d.]+)"?.{0,200}?"roe"\s*:\s*"?([\d.]+)"?/gi;
  let m;
  while ((m = re.exec(blob)) !== null) {
    const code = m[1];
    const pe = Number(m[2]);
    const roeRaw = Number(m[3]);
    if (Number.isFinite(pe) && pe>0 && pe<1000) {
      let roe = null;
      if (Number.isFinite(roeRaw)) {
        roe = roeRaw > 1 ? roeRaw/100 : roeRaw;
        if (!(roe>0 && roe<1)) roe = null;
      }
      map[code] = { pe, roe };
    }
  }
  return map;
}
async function getFromVC(code){
  if (!VC_CACHE) VC_CACHE = await fetchValueCenterMap();
  return VC_CACHE[code] || null;
}

// ---------- P/E 抓取（含回退） ----------
async function peHS300(){
  const vc = await getFromVC("SH000300");
  if (vc?.pe) return { v: vc.pe, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/djmodule/value-center?channel=1300100141","ValueCenter SH000300")' };
  return await peHS300_fallback();
}
async function peSPX(){
  const vc = await getFromVC("SP500");
  if (vc?.pe) return { v: vc.pe, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/djmodule/value-center?channel=1300100141","ValueCenter SP500")' };
  return await peSPX_fallback();
}
async function peChinaInternet(){
  const vc = await getFromVC("CSIH30533");
  if (vc?.pe) return { v: vc.pe, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/djmodule/value-center?channel=1300100141","ValueCenter CSIH30533")' };
  return await peCXIN_fallback();
}
async function peHSTECH(){
  const vc = await getFromVC("HSTECH");
  if (vc?.pe) return { v: vc.pe, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/djmodule/value-center?channel=1300100141","ValueCenter HSTECH")' };
  const url = "https://danjuanfunds.com/dj-valuation-table-detail/HSTECH";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:12000 });
    if(r.ok){
      const h=await r.text();
      const text=strip(h);
      let m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i) || text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan HSTECH")` }; }
    }
  }catch(e){ dbg("peHSTECH HTTP err", e.message); }
  if(PE_OVERRIDE_HSTECH!=null) return { v:PE_OVERRIDE_HSTECH, tag:"兜底", link:`=HYPERLINK("${url}","Danjuan HSTECH")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Danjuan HSTECH")` };
}

// —— 回退实现（略去冗余注释） ——
async function peHS300_fallback(){
  const url = "https://danjuanfunds.com/index-detail/SH000300";
  try{
    if (USE_PW) {
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(url, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(2000);
      let text = await pg.locator("body").innerText().catch(()=> "");
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
      if (Number.isFinite(val) && val > 0 && val < 1000)
        return { v: val, tag: "真实", link: `=HYPERLINK("${url}","Danjuan HS300")` };
    }
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:12000 });
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let m=text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(!m) m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan HS300")` }; }
    }
  }catch(e){ dbg("peHS300_fallback err", e.message); }
  if(PE_OVERRIDE_CN!=null) return { v:PE_OVERRIDE_CN, tag:"兜底", link:`=HYPERLINK("${url}","Danjuan HS300")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Danjuan HS300")` };
}
async function peSPX_fallback(){
  const urlIdx = "https://danjuanfunds.com/index-detail/SP500";
  const urlVal = "https://danjuanfunds.com/dj-valuation-table-detail/SP500";
  try{
    if (USE_PW) {
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(urlIdx, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(2000);
      let text = await pg.locator("body").innerText().catch(()=> "");
      let m = text && text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(m){ const v=Number(m[1]); await br.close();
        if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${urlIdx}","Danjuan SP500")` }; }
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
      if(Number.isFinite(v2)&&v2>0&&v2<1000) return { v:v2, tag:"真实", link:`=HYPERLINK("${urlIdx}","Danjuan SP500")` };
    }
    const r=await fetch(urlVal,{ headers:{ "User-Agent":UA }, timeout:12000 });
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let m=text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(!m) m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` }; }
    }
  }catch(e){ dbg("peSPX_fallback err", e.message); }
  if(PE_OVERRIDE_SPX!=null) return { v:PE_OVERRIDE_SPX, tag:"兜底", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
}
async function peCXIN_fallback(){
  const url = "https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i) || text.match(/PE\s*\d{2}-\d{2}\s*(\d{1,3}\.\d{1,2})/);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` }; }
    }
  }catch(e){ dbg("peCXIN_fallback err", e.message); }
  if(PE_OVERRIDE_CXIN!=null) return { v:PE_OVERRIDE_CXIN, tag:"兜底", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Danjuan CSIH30533")` };
}

// ---------- ROE(TTM) 抓取（通用） ----------
async function roeFromDanjuan(urls){
  // Playwright 优先（更稳）
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
        let m = body && body.match(/ROE[^%\d]{0,40}(\d{1,2}(?:\.\d{1,2})?)\s*%/i);
        if(m){ const v=Number(m[1])/100; await br.close(); if(v>0.03 && v<0.40) return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
        // DOM 邻近（含 ROE 的块里找最近的百分数）
        const v2 = await pg.evaluate(()=>{
          const re=/(\d{1,2}(?:\.\d{1,2})?)\s*%/;
          for(const el of Array.from(document.querySelectorAll("body *"))){
            const t=(el.textContent||"").trim();
            if(!/ROE\b/i.test(t)) continue;
            const m=t.match(re);
            if(m){ const x=parseFloat(m[1]); if(Number.isFinite(x)) return x/100; }
          }
          return null;
        }).catch(()=> null);
        if(Number.isFinite(v2) && v2>0.03 && v2<0.40) { await br.close(); return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      }
      await br.close();
    }catch(e){ dbg("roeFromDanjuan PW err", e.message); }
  }
  // HTTP 兜底：先 JSON 再文本邻近
  for(const url of urls){
    try{
      const r = await fetch(url, { headers:{ "User-Agent": UA }, timeout:15000 });
      if(!r.ok) continue;
      const h = await r.text(); const text = strip(h);
      let j = h.match(/"roe_ttm"\s*:\s*"?(\d{1,2}(?:\.\d{1,2})?)"?/i) || h.match(/"roe"\s*:\s*"?(\d{1,2}(?:\.\d{1,2})?)"?/i);
      if(j){ const v = Number(j[1])/100; if(v>0.03 && v<0.40) return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      const idx = text.search(/ROE\b/i);
      if(idx>=0){
        const right = text.slice(idx, idx+220);
        const m = right.match(/(\d{1,2}(?:\.\d{1,2})?)\s*%/);
        if(m){ const v = Number(m[1])/100; if(v>0.03 && v<0.40) return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      }
    }catch(e){ dbg("roeFromDanjuan HTTP err", e.message); }
  }
  return { v:"", tag:"兜底", link:"—" };
}

// —— 专用：中概互联网 ROE（保留点击 tab 的更稳实现） ——
async function roeCXIN(){
  const url = "https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533";
  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(url, { waitUntil: 'domcontentloaded' });

      const tabSel = ['text=/^\\s*ROE\\s*走?势?\\s*$/i','text=/^\\s*ROE\\s*$/i','text=/ROE/'];
      for (const sel of tabSel) { try { await pg.locator(sel).first().click({ timeout: 900 }); break; } catch {} }
      await pg.waitForTimeout(1000);

      const val = await pg.evaluate(()=>{
        const blocks = Array.from(document.querySelectorAll("body *")).filter(el=>/ROE\b/i.test((el.textContent||"")));
        for(const el of blocks){
          const txt=(el.textContent||"").replace(/\s+/g," ");
          const m=txt.match(/ROE[^%\d]{0,40}(\d{1,2}(?:\.\d{1,2})?)\s*%/i);
          if(m){ const x=parseFloat(m[1]); if(isFinite(x)) return x; }
        }
        return null;
      });
      await br.close();
      if(Number.isFinite(val) && val>3 && val<40) return { v: val/100, tag:"真实", link:`=HYPERLINK("${url}","ROE")` };
    }catch(e){ dbg("roeCXIN PW err", e.message); }

  }
  // HTTP 兜底
  try{
    const r=await fetch(url,{ headers:{ "User-Agent": UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text(); const text=strip(h);
      let j = h.match(/"roe_ttm"\s*:\s*"?(\d{1,2}(?:\.\d{1,2})?)"?/i) || h.match(/"roe"\s*:\s*"?(\d{1,2}(?:\.\d{1,2})?)"?/i);
      if(j){ const v = Number(j[1])/100; if(v>0.03 && v<0.40) return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      const idx = text.search(/ROE\b/i);
      if(idx>=0){
        const right = text.slice(idx, idx+240);
        const m = right.match(/(\d{1,2}(?:\.\d{1,2})?)\s*%/);
        if(m){ const v = Number(m[1])/100; if(v>0.03 && v<0.40) return { v, tag:"真实", link:`=HYPERLINK("${url}","ROE")` }; }
      }
    }
  }catch(e){ dbg("roeCXIN HTTP err", e.message); }
  return { v:"", tag:"兜底", link:"—" };
}

// ---------- 写块（判定基于区间） ----------
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink, roeRes){
  const { sheetTitle, sheetId } = await ensureToday();

  const pe = (peRes?.v==="" || peRes?.v==null) ? null : Number(peRes?.v);
  const rf = Number.isFinite(rfRes?.v) ? rfRes.v : null;
  const target = (label==="沪深300") ? ERP_TARGET_CN : erpStar;
  const roe = Number.isFinite(roeRes?.v) ? roeRes.v : null;

  const ep = Number.isFinite(pe) ? 1/pe : null;
  const factor = (roe!=null && roe>0) ? (roe/ROE_BASE) : 1;
  const factorDisp = (roe!=null && roe>0) ? Number(factor.toFixed(2)) : "";

  const peBuy  = (rf!=null && target!=null) ? Number((1/(rf+target+DELTA)*factor).toFixed(2)) : null;
  const peSell = (rf!=null && target!=null && (rf+target-DELTA)>0) ? Number((1/(rf+target-DELTA)*factor).toFixed(2)) : null;
  const fairRange = (peBuy!=null && peSell!=null) ? `${peBuy} ~ ${peSell}` : "";

  let status="需手动更新";
  if(Number.isFinite(pe) && peBuy!=null && peSell!=null){
    if (pe <= peBuy) status="🟢 买点（低估）";
    else if (pe >= peSell) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const values = [
    ["指数", label, "真实", "宽基/行业指数估值分块", peRes?.link || "—"],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peRes?.tag || (Number.isFinite(pe)?"真实":"兜底"), "估值来源", peRes?.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rf!=null?"真实":"兜底", (label==="沪深300"?"有知有行 10Y":"Investing.com 10Y"), rfRes?.link || "—"],
    ["目标 ERP*", (label==="沪深300"? ERP_TARGET_CN : (Number.isFinite(target)?target:"")), (label==="沪深300"?"真实":(Number.isFinite(target)?"真实":"兜底")),
      (label==="沪深300"?"建议参考达摩达兰":"达摩达兰"), erpLink || '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换（说明用，不定义卖点）","—"],
    ["买点PE上限（含ROE因子）", peBuy ?? "", (peBuy!=null)?"真实":"兜底", "买点=1/(r_f+ERP*+δ)×factor","—"],
    ["卖点PE下限（含ROE因子）", peSell ?? "", (peSell!=null)?"真实":"兜底", "卖点=1/(r_f+ERP*−δ)×factor","—"],
    ["合理PE区间（含ROE因子）", fairRange, (peBuy!=null && peSell!=null)?"真实":"兜底", "买点上限 ~ 卖点下限","—"],
    ["ROE（TTM）", roe ?? "", (roe!=null)?"真实":"兜底", "盈利能力（小数，显示为百分比）", roeRes?.link || "—"],
    ["ROE基准（可配 env.ROE_BASE）", ROE_BASE, "真实", "默认 0.12 = 12%","—"],
    ["ROE倍数因子 = ROE/ROE基准", factorDisp, (factorDisp!=="")?"真实":"兜底", "示例 16.4%/12% = 1.36","—"],
    ["说明（公式）", "见右", "真实", "买点=1/(r_f+ERP*+δ)×factor；卖点=1/(r_f+ERP*−δ)×factor；合理区间=买点~卖点","—"],
    ["判定", status, (Number.isFinite(pe) && peBuy!=null && peSell!=null)?"真实":"兜底", "基于 P/E 与区间","—"],
  ];

  const totalRows = values.length;
  const endRow = startRow + totalRows - 1;

  await write(`'${sheetTitle}'!A${startRow}:E${endRow}`, values);

  const base = startRow - 1;
  const pctRowsAbs = [base+2, base+3, base+4, base+5, base+9, base+10];
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
        { repeatCell: {
            range: { sheetId, startRowIndex: base+0, endRowIndex: base+1, startColumnIndex:0, endColumnIndex:5 },
            cell: { userEnteredFormat:{ backgroundColor:{ red:0.95, green:0.95, blue:0.95 }, textFormat:{ bold:true } } },
            fields: "userEnteredFormat(backgroundColor,textFormat)"
        }},
        { updateBorders: {
            range: { sheetId, startRowIndex: base, endRowIndex: base + totalRows, startColumnIndex:0, endColumnIndex:5 },
            top:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
            bottom:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
            left:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
            right:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } }
        }}
      ]
    }
  });

  return { nextRow: endRow + 2, judgment: status, pe };
}

// ---------- 邮件 ----------
async function sendEmailIfEnabled(lines){
  const {
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
    MAIL_TO, MAIL_FROM_NAME, MAIL_FROM_EMAIL, FORCE_EMAIL
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

  try{ dbg("[MAIL] verify start", { host: SMTP_HOST, user: SMTP_USER, to: MAIL_TO }); await transporter.verify(); dbg("[MAIL] verify ok"); }
  catch(e){ console.error("[MAIL] verify fail:", e); if(!FORCE_EMAIL) return; console.error("[MAIL] continue due to FORCE_EMAIL=1"); }

  const fromEmail = MAIL_FROM_EMAIL || SMTP_USER;
  const from = MAIL_FROM_NAME ? `${MAIL_FROM_NAME} <${fromEmail}>` : fromEmail;
  const subject = `Valuation Daily — ${todayStr()} (${TZ})`;
  const text = [
    `Valuation Daily — ${todayStr()} (${TZ})`,
    ...lines.map(s=>`• ${s}`),
    ``, `See sheet "${todayStr()}" for thresholds & judgments.`
  ].join('\n');
  const html = [
    `<h3>Valuation Daily — ${todayStr()} (${TZ})</h3>`,
    `<ul>${lines.map(s=>`<li>${s}</li>`).join("")}</ul>`,
    `<p>See sheet "${todayStr()}" for thresholds & judgments.</p>`
  ].join("");

  dbg("[MAIL] send start", { subject, to: MAIL_TO, from });
  try{
    const info = await transporter.sendMail({ from, to: MAIL_TO, subject, text, html });
    console.log("[MAIL] sent", { messageId: info.messageId, response: info.response });
  }catch(e){ console.error("[MAIL] send error:", e); }
}

// ---------- Main ----------
(async()=>{
  console.log("[INFO] Run start", todayStr(), "USE_PLAYWRIGHT=", USE_PW, "TZ=", TZ);

  let row=1;
  const { sheetTitle, sheetId } = await ensureToday();
  await clearTodaySheet(sheetTitle, sheetId);

  // 先抓 Value Center（若成功，后续命中缓存）
  VC_CACHE = await fetchValueCenterMap();

  // 1) HS300
  const pe_hs = await peHS300();  const rf_cn  = await rfCN();  const roe_hs = await roeFromDanjuan(["https://danjuanfunds.com/index-detail/SH000300"]);
  let r = await writeBlock(row,"沪深300", pe_hs, rf_cn, null, null, null, roe_hs);
  row = r.nextRow; const j_hs = r.judgment; const pv_hs = r.pe;

  // 2) SP500
  const rf_us  = await rfUS(); const { v:erp_us_v, tag:erp_us_tag, link:erp_us_link } = await erpUS();
  const pe_spx = await peSPX(); const roe_spx = await roeFromDanjuan(["https://danjuanfunds.com/dj-valuation-table-detail/SP500","https://danjuanfunds.com/index-detail/SP500"]);
  r = await writeBlock(row,"标普500", pe_spx, rf_us, erp_us_v, erp_us_tag, erp_us_link, roe_spx);
  row = r.nextRow; const j_sp = r.judgment; const pv_sp = r.pe;

  // 3) Nikkei（ROE 如未覆写则因子=1）
  const pe_nk = await peNikkei(); const rf_jp  = await rfJP(); const { v:erp_jp_v, tag:erp_jp_tag, link:erp_jp_link } = await erpJP();
  const roe_nk = (ROE_JP!=null) ? { v:ROE_JP, tag:"覆写", link:"—" } : { v:null, tag:"兜底", link:"—" };
  r = await writeBlock(row,"日经指数", pe_nk, rf_jp, erp_jp_v, erp_jp_tag, erp_jp_link, roe_nk);
  row = r.nextRow; const j_nk = r.judgment; const pv_nk = r.pe;

  // 4) 中概互联网
  const pe_cxin = await peChinaInternet(); const rf_cn2  = await rfCN(); const { v:erp_cn_v, tag:erp_cn_tag, link:erp_cn_link } = await erpCN();
  const roe_cxin = await roeCXIN();
  r = await writeBlock(row,"中概互联网", pe_cxin, rf_cn2, erp_cn_v, erp_cn_tag, erp_cn_link, roe_cxin);
  row = r.nextRow; const j_cx = r.judgment; const pv_cx = r.pe;

  // 5) 恒生科技
  const pe_hk  = await peHSTECH(); const rf_hk10 = await rfHK(); const { v:erp_hk_v, tag:erp_hk_tag, link:erp_hk_link } = await erpHK();
  const roe_hk = await (async()=>{ const vc = await getFromVC("HSTECH"); if(vc?.roe) return { v:vc.roe, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/djmodule/value-center?channel=1300100141","ValueCenter")' }; const r=numOr(process.env.ROE_HSTECH,null); return (r!=null)?{v:r,tag:"覆写",link:"—"}:{v:null,tag:"兜底",link:"—"}; })();
  r = await writeBlock(row,"恒生科技", pe_hk, rf_hk10, erp_hk_v, erp_hk_tag, erp_hk_link, roe_hk);
  row = r.nextRow; const j_hsTech = r.judgment; const pv_hsTech = r.pe;

  console.log("[DONE]", todayStr(), {
    hs300_pe: pe_hs?.v, spx_pe: pe_spx?.v, nikkei_pe: pe_nk?.v, cxin_pe: pe_cxin?.v, hstech_pe: pe_hk?.v
  });

  const lines = [
    `HS300 PE: ${pv_hs ?? "-"} → ${j_hs ?? "-"}`,
    `SPX PE: ${pv_sp ?? "-"} → ${j_sp ?? "-"}`,
    `Nikkei PE: ${pv_nk ?? "-"} → ${j_nk ?? "-"}`,
    `China Internet PE: ${pv_cx ?? "-"} → ${j_cx ?? "-"}`,
    `HSTECH PE: ${pv_hsTech ?? "-"} → ${j_hsTech ?? "-"}`
  ];
  await sendEmailIfEnabled(lines);
})();
