// Version: V2.3
// Change:
// 1) 将 δ（容忍带）等价映射到 P/E 空间：新增 买点PE上限 / 卖点PE下限 / 合理PE区间 三条阈值
// 2) 保留原“对应P/E上限 = 1/(r_f + ERP*)”作为中枢参考
// 3) 其他逻辑与 V2.2 保持一致（包含 Nikkei DOM/HTML 抓取与 “空值不写0” 修复）

// HS300 + S&P500 + Nikkei225 —— 三块详表；HS300 仅用 index-detail/SH000300；SPX 优先 index-detail；Nikkei 用官方档案页：Index Weight Basis
// E/P、r_f、隐含ERP、目标ERP*、容忍带δ 显示为百分比；大量 [DEBUG]；绝不写 0。

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
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);   // HS300（可通过环境变量覆盖）
const DELTA         = numOr(process.env.DELTA,      0.005);

// ---------- 兜底（小数） ----------
const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);
const RF_US = numOr(process.env.RF_US,       0.0425);
const RF_JP = numOr(process.env.RF_JP,       0.0100);          // 日本10Y兜底（示例 1.00%）

const PE_OVERRIDE_CN      = (()=>{ const s=(process.env.PE_OVERRIDE??"").trim();           return s?Number(s):null; })();
const PE_OVERRIDE_SPX     = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim();       return s?Number(s):null; })();
const PE_OVERRIDE_NIKKEI  = (()=>{ const s=(process.env.PE_OVERRIDE_NIKKEI??"").trim();    return s?Number(s):null; })();

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
      dbg("rfCN instrument-price-last", v);
      if (!Number.isFinite(v)) {
        const text = strip(h);
        const near = text.match(/(收益率|Yield)[^%]{0,40}?(\d{1,2}\.\d{1,4})\s*%/i) ||
                     text.match(/(\d{1,2}\.\d{1,4})\s*%/);
        if (near) v = Number(near[2] || near[1]) / 100;
      }
      if (Number.isFinite(v) && v > 0 && v < 1)
        return { v, tag: "真实", link: `=HYPERLINK("${url}","CN 10Y (Investing)")` };
    }
  } catch (e) { dbg("rfCN investing err", e.message); }

  // B) 有知有行
  try {
    const url = "https://youzhiyouxing.cn/data";
    const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" }, timeout: 8000 });
    if (r.ok) {
      const html = await r.text();
      let m = html.match(/10年期国债到期收益率[^%]{0,200}?(\d+(?:\.\d+)?)\s*%/);
      if (!m) {
        const all = [...html.matchAll(/(\d+(?:\.\d+)?)\s*%/g)].map(x => Number(x[1])).filter(Number.isFinite);
        if (all.length) m = [null, Math.max(...all).toString()];
      }
      if (m) {
        const v = Number(m[1]) / 100;
        if (Number.isFinite(v) && v > 0 && v < 1)
          return { v, tag: "真实", link: `=HYPERLINK("${url}","Youzhiyouxing")` };
      }
    }
  } catch (e) { dbg("rfCN yzyx err", e.message); }

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
    } catch (e) { dbg("rfUS err", url, e.message); }
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

// ========== Danjuan：HS300 & SPX ==========
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
        const lines = (text || "").split(/\n+/);
        for (const line of lines) {
          if (/PE/i.test(line) && !/分位/.test(line)) {
            const mm = line.match(/(\d{1,3}\.\d{1,2})/);
            if (mm) { val = Number(mm[1]); break; }
          }
        }
      }
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
      if (!m) m = text.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
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
      if(!m) m=text.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
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

// ========== Nikkei 225 ==========
async function peNikkei(){
  const url = "https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per";

  // A) Playwright DOM（优先，最稳）
  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'en-US', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(url, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(1500);

      // 选择数据表最后一行第 3 列（Index Weight Basis）
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
      dbg("Nikkei PW value", v);
      if(Number.isFinite(v) && v>0 && v<1000){
        return { v, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
      }
    }catch(e){ dbg("Nikkei PW error", e.message); }
  }

  // B) HTML：逐 <tr> 解析
  try{
    const r = await fetch(url, { headers:{ "User-Agent": UA, "Referer":"https://www.google.com" }, timeout:15000 });
    dbg("Nikkei page status", r.status);
    if(!r.ok) throw new Error("status not ok");
    const h = await r.text();

    // 按行拆分 <tr>，每行再抽 3 个 <td>；第三个即 Index Weight Basis
    const trs = [...h.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)].map(m=>m[1]);
    let lastVal = null, lastDate = null;
    for(const tr of trs){
      const tds = [...tr.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map(m=>m[1].replace(/<[^>]*>/g,"").trim());
      if(tds.length>=3 && /[A-Za-z]{3}\/\d{2}\/\d{4}/.test(tds[0])){
        lastDate = tds[0];
        const n = parseFloat(tds[2].replace(/,/g,""));
        if(Number.isFinite(n)) lastVal = n;   // 记录到最后一条
      }
    }
    dbg("Nikkei HTML last", { lastDate, lastVal });
    if(Number.isFinite(lastVal) && lastVal>0 && lastVal<1000){
      return { v:lastVal, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
    }
  }catch(e){ dbg("Nikkei fetch error", e.message); }

  // C) 最终兜底
  if(PE_OVERRIDE_NIKKEI!=null) return { v: PE_OVERRIDE_NIKKEI, tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
}

// ---------- 写单块 ----------
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink){
  const { sheetTitle, sheetId } = await ensureToday();

  // 严禁把 "" 变成 0
  const pe = (peRes==null || peRes.v==="" || peRes.v==null) ? null : Number(peRes.v);
  const rf = Number.isFinite(rfRes?.v) ? rfRes.v : null;
  const target = (label==="沪深300") ? ERP_TARGET_CN : erpStar;

  const ep = Number.isFinite(pe) ? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rf)) ? (ep - rf) : null;
  const peLimit = (Number.isFinite(rf) && Number.isFinite(target)) ? Number((1/(rf+target)).toFixed(2)) : null;

  // —— 新增三条 P/E 阈值（把 δ 映射到 P/E 空间）——
  const denomBuy  = (Number.isFinite(rf) && Number.isFinite(target)) ? (rf + target + DELTA) : null;
  const denomSell = (Number.isFinite(rf) && Number.isFinite(target)) ? (rf + target - DELTA) : null;

  const peBuyUpper  = (denomBuy  != null && denomBuy  > 0) ? Number((1/denomBuy ).toFixed(2)) : null; // 低估买点的 P/E 上限
  const peSellLower = (denomSell != null && denomSell > 0) ? Number((1/denomSell).toFixed(2)) : null; // 高估卖点的 P/E 下限

  dbg(`${label} values`, { pe, rf, target, ep, implied, peLimit, peBuyUpper, peSellLower });

  let status="需手动更新";
  if (implied!=null && Number.isFinite(target)) {
    if (implied >= target + DELTA) status="🟢 买点（低估）";
    else if (implied <= target - DELTA) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const fairRange =
    (peBuyUpper!=null && peSellLower!=null)
      ? `${peBuyUpper} ~ ${peSellLower}`
      : "";

  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数", label, "真实", "宽基指数估值分块", peRes?.link || "—"],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peRes?.tag || (Number.isFinite(pe)?"真实":"兜底"), "估值来源", peRes?.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rf ?? "", rf!=null?"真实":"兜底", (label==="沪深300"?"有知有行 10Y":"Investing.com 10Y"), rfRes?.link || "—"],
    ["隐含ERP = E/P − r_f", implied ?? "", (implied!=null)?"真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", (label==="沪深300"? ERP_TARGET_CN : (Number.isFinite(target)?target:"")), (label==="沪深300"?"真实":(Number.isFinite(target)?"真实":"兜底")),
      (label==="沪深300"?"建议参考达摩达兰":"达摩达兰"), erpLink || '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", (peLimit!=null)?"真实":"兜底", "直观对照（中枢）","—"],
    // —— 新增三条：把 δ 映射进 P/E 空间 ——
    ["买点PE上限 = 1/(r_f + ERP* + δ)", peBuyUpper ?? "", (peBuyUpper!=null)?"真实":"兜底", "低估买点的直观阈值","—"],
    ["卖点PE下限 = 1/(r_f + ERP* − δ)", peSellLower ?? "", (peSellLower!=null)?"真实":"兜底", "高估卖点的直观阈值（需分母>0）","—"],
    ["合理PE区间（买点上限 ~ 卖点下限）", fairRange, (peBuyUpper!=null && peSellLower!=null)?"真实":"兜底", "合理持有的直观区间","—"],
    ["判定", status, (implied!=null && Number.isFinite(target))?"真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];

  // 写入
  const end = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`, rows);

  // 百分比格式化：E/P、r_f、隐含ERP、ERP*、δ
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        {
          repeatCell: {
            range: {
              sheetId,
              startRowIndex: (startRow - 1) + 3,  // 第4行(E/P)
              endRowIndex:   (startRow - 1) + 8,  // 第8行(δ) 之后不含
              startColumnIndex: 1,                // B 列
              endColumnIndex:   2
            },
            cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00%" } } },
            fields: "userEnteredFormat.numberFormat"
          }
        }
        // （P/E 阈值为“倍数”，保持默认数字格式即可；如需两位小数，可再添加一个 repeatCell）
      ]
    }
  });

  return end + 2;
}

// ========== Main ==========
(async()=>{
  console.log("[INFO] Run start", todayStr(), "USE_PLAYWRIGHT=", USE_PW, "TZ=", TZ);

  let row=1;

  // HS300（中国10Y + ERP_TARGET_CN）
  const pe_hs = await peHS300();
  const rf_cn = await rfCN();
  row = await writeBlock(row,"沪深300", pe_hs, rf_cn, null, null, null);

  // SPX（美国10Y + ERP(US)）
  const rf_us  = await rfUS();
  const { v:erp_us_v, tag:erp_us_tag, link:erp_us_link } = await erpUS();
  const pe_spx = await peSPX();
  row = await writeBlock(row,"标普500", pe_spx, rf_us, erp_us_v, erp_us_tag, erp_us_link);

  // Nikkei 225（日经指数：日本10Y + ERP(Japan)）
  const pe_nk = await peNikkei();
  const rf_jp = await rfJP();
  const { v:erp_jp_v, tag:erp_jp_tag, link:erp_jp_link } = await erpJP();
  row = await writeBlock(row,"日经指数", pe_nk, rf_jp, erp_jp_v, erp_jp_tag, erp_jp_link);

  console.log("[DONE]", todayStr(), { hs300_pe: pe_hs?.v, spx_pe: pe_spx?.v, nikkei_pe: pe_nk?.v });
})();
