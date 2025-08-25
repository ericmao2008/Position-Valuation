/**
 * Version History
 * V2.7.4
 *  - 统一改为 “表格解析” 的 Value Center 抓取（仅 HS300/SP500/CSIH30533/HSTECH）：
 *      * 通过 <a href="/dj-valuation-table-detail/<CODE>"> 锁定对应 <tr>
 *      * 第 2 列取 PE（小数）；第 7 列取 ROE（百分比 → 小数）
 *      * HTTP 优先，如需再 Playwright 打开同页读取 page.content() 再解析
 *  - 口径：HS300/CSIH30533/HSTECH → r_f=中国10Y，ERP*=China；SP500 → r_f=US10Y，ERP*=US
 *  - Nikkei 仍用官方档案页 PER；ROE 暂用 ROE_JP（小数）可覆写
 *  - 判定：基于 P/E 与 [买点, 卖点] 区间；邮件正文包含判定；DEBUG 保留
 *
 * V2.7.3
 *  - 修复：重复 import nodemailer
 *
 * V2.7.2
 *  - 修复 peNikkei 未定义；Value Center-only（除 Nikkei）；HSTECH 与中概口径一致
 *
 * V2.7.1
 *  - 修复 roeFromDanjuan 未定义；保留 Value Center 优先、邮件判定、恒生科技分块
 *
 * V2.7.0-test
 *  - 新增恒生科技（HSTECH）；Value Center 优先抓取；邮件正文加入判定
 *
 * V2.6.11
 *  - 修复：P/E 抓取函数占位导致 undefined；恢复并加固四个 pe 函数
 *
 * V2.6.10
 *  - 修复：CSIH30533 的 ROE(TTM) 丢失（点击 ROE tab + JSON 优先 + 3%~40% 过滤）
 *  - 邮件：支持 MAIL_FROM_EMAIL/MAIL_FROM_NAME；text+html；verify + DEBUG
 *
 * V2.6.9
 *  - 判定：基于 P/E 与 [买点, 卖点] 区间；内建邮件 DEBUG（verify/send/FORCE_EMAIL）
 *
 * V2.6.8
 *  - 修复：中概 ROE 偶发抓成 30%（更严格匹配与范围过滤）
 *
 * V2.6.7
 *  - 去除“中枢（对应P/E上限）”；仅保留买点/卖点/合理区间；公式写入说明
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
 *  - CSIH30533 切中国口径：r_f=中国10Y，ERP*=China
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
const text2num = (s)=>{ const x=parseFloat((s||"").replace(/,/g,"").trim()); return Number.isFinite(x)?x:null; };
const pct2dec = (s)=>{ const m=(s||"").match(/(-?\d+(?:\.\d+)?)\s*%/); if(!m) return null; const v=Number(m[1])/100; return (v>0 && v<1)? v : null; };

// ---------- 参数 ----------
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.005);
const ROE_BASE      = numOr(process.env.ROE_BASE,   0.12);

// r_f 兜底（HS300/CSIH/HSTECH 用中国10Y；SPX 用美国10Y；Nikkei 用日本10Y）
const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);
const RF_US = numOr(process.env.RF_US,       0.0425);
const RF_JP = numOr(process.env.RF_JP,       0.0100);

// 覆写 & ROE_JP
const PE_OVERRIDE_CN      = (()=>{ const s=(process.env.PE_OVERRIDE??"").trim();           return s?Number(s):null; })();
const PE_OVERRIDE_SPX     = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim();       return s?Number(s):null; })();
const PE_OVERRIDE_CXIN    = (()=>{ const s=(process.env.PE_OVERRIDE_CXIN??"").trim();      return s?Number(s):null; })();
const PE_OVERRIDE_HSTECH  = (()=>{ const s=(process.env.PE_OVERRIDE_HSTECH??"").trim();    return s?Number(s):null; })();
const ROE_JP = numOr(process.env.ROE_JP, null);   // 日经 ROE 覆写（小数）

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

// ---------- Value Center 表格解析 ----------
const VC_URL = "https://danjuanfunds.com/djmodule/value-center?channel=1300100141";
// code 与链接锚点
const VC_LINK = {
  SH000300: "/dj-valuation-table-detail/SH000300",
  SP500:    "/dj-valuation-table-detail/SP500",
  CSIH30533:"/dj-valuation-table-detail/CSIH30533",
  HSTECH:   "/dj-valuation-table-detail/HSTECH"
};
// 解析整页表格为 map：{ code -> {pe, roe} }
function parseValueCenterTable(html){
  const map = {};
  const rows = [...html.matchAll(/<tr[\s\S]*?<\/tr>/gi)].map(m=>m[0]);
  for(const [code, href] of Object.entries(VC_LINK)){
    const row = rows.find(tr => tr.includes(href));
    if(!row) continue;
    // 抽取本行所有 <td>
    const tds = [...row.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map(m=>m[1].replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim());
    if(tds.length < 7) continue; // 防御：列不足
    const pe  = text2num(tds[1]);           // 第 2 列
    const roe = pct2dec(tds[6]);            // 第 7 列（百分比）
    if(Number.isFinite(pe) && pe>0 && pe<1000){
      map[code] = { pe, roe: (roe>0 && roe<1)? roe : null };
    }
  }
  return map;
}
async function fetchVCByTable(){
  // HTTP 优先
  try{
    const r = await fetch(VC_URL, { headers:{ "User-Agent": UA }, timeout: 15000 });
    if(r.ok){
      const h = await r.text();
      const m = parseValueCenterTable(h);
      if(Object.keys(m).length){ dbg("VC table via HTTP", m); return m; }
    }
  }catch(e){ dbg("VC table HTTP err", e.message); }

  // Playwright 兜底
  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(VC_URL, { waitUntil: 'domcontentloaded' });
      await pg.waitForTimeout(1800);
      const html = await pg.content();
      await br.close();
      const m = parseValueCenterTable(html);
      if(Object.keys(m).length){ dbg("VC table via PW", m); return m; }
    }catch(e){ dbg("VC table PW err", e.message); }
  }
  return {};
}
let VC_CACHE = null;
async function getVC(code){
  if(!VC_CACHE) VC_CACHE = await fetchVCByTable();
  return VC_CACHE[code] || null;
}

// ---------- r_f / ERP* ----------
async function rfCN(){ try{
  const url="https://cn.investing.com/rates-bonds/china-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const near=t.match(/(\d{1,2}\.\d{1,4})\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:'=HYPERLINK("https://cn.investing.com/rates-bonds/china-10-year-bond-yield","CN 10Y")' };
  }
}catch{} return { v:RF_CN, tag:"兜底", link:"—" }; }
async function rfUS(){ try{
  const url="https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const near=t.match(/(\d{1,2}\.\d{1,4})\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:'=HYPERLINK("https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield","US 10Y")' };
  }
}catch{} return { v:RF_US, tag:"兜底", link:"—" }; }
async function rfJP(){ try{
  const url="https://cn.investing.com/rates-bonds/japan-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const near=t.match(/(\d{1,2}\.\d{1,4})\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:'=HYPERLINK("https://cn.investing.com/rates-bonds/japan-10-year-bond-yield","JP 10Y")' };
  }
}catch{} return { v:RF_JP, tag:"兜底", link:"—" }; }

async function erpCN(){ // China ERP*
  const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const row=h.split(/<\/tr>/i).find(tr=> /China/i.test(tr)) || "";
      const p=[...row.replace(/<[^>]+>/g," ").matchAll(/(\d{1,2}\.\d{1,2})\s*%/g)].map(m=>Number(m[1]));
      const v=p.find(x=>x>2 && x<10);
      if(v!=null) return { v: v/100, tag:"真实", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran China")' };
    }
  }catch{}
  return { v:0.0527, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' };
}
async function erpUS(){ // US ERP*
  const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const row=h.split(/<\/tr>/i).find(tr=> /(United\s*States|USA)/i.test(tr)) || "";
      const p=[...row.replace(/<[^>]+>/g," ").matchAll(/(\d{1,2}\.\d{1,2})\s*%/g)].map(m=>Number(m[1]));
      const v=p.find(x=>x>2 && x<10);
      if(v!=null) return { v: v/100, tag:"真实", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran US")' };
    }
  }catch{}
  return { v:0.0433, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' };
}
async function erpJP(){ // Japan ERP*
  const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const row=h.split(/<\/tr>/i).find(tr=> /Japan/i.test(tr)) || "";
      const p=[...row.replace(/<[^>]+>/g," ").matchAll(/(\d{1,2}\.\d{1,2})\s*%/g)].map(m=>Number(m[1]));
      const v=p.find(x=>x>2 && x<10);
      if(v!=null) return { v: v/100, tag:"真实", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran JP")' };
    }
  }catch{}
  return { v:0.0527, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' };
}

// ---------- 通过 VC 取值（除 Nikkei） ----------
async function peFromVC(code, label){
  const rec = await getVC(code);
  if(rec?.pe) return { v: rec.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","${label}")` };
  // 给一个可控的兜底（如设了 override）
  const ov = { SH000300: PE_OVERRIDE_CN, SP500: PE_OVERRIDE_SPX, CSIH30533: PE_OVERRIDE_CXIN, HSTECH: PE_OVERRIDE_HSTECH }[code];
  return { v: ov??"", tag:"兜底", link:`=HYPERLINK("${VC_URL}","${label}")` };
}
async function roeFromVC(code){
  const rec = await getVC(code);
  if(rec?.roe) return { v: rec.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","ValueCenter")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${VC_URL}","ValueCenter")` };
}

// ---------- Nikkei 专用：PE ----------
async function peNikkei(){
  const url = "https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per";
  // Playwright 优先
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
      if(Number.isFinite(v) && v>0 && v<1000)
        return { v, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
    }catch(e){ dbg("peNikkei PW err", e.message); }
  }
  // HTTP 兜底
  try{
    const r = await fetch(url, { headers:{ "User-Agent": UA, "Referer":"https://www.google.com" }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const trs=[...h.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)].map(m=>m[1]); let lastVal=null;
      for(const tr of trs){
        const tds=[...tr.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map(m=>m[1].replace(/<[^>]+>/g,"").trim());
        if(tds.length>=3 && /[A-Za-z]{3}\/\d{2}\/\d{4}/.test(tds[0])){
          const n=parseFloat(tds[2].replace(/,/g,"")); if(Number.isFinite(n)) lastVal=n;
        }
      }
      if(Number.isFinite(lastVal) && lastVal>0 && lastVal<1000)
        return { v:lastVal, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
    }
  }catch(e){ dbg("peNikkei HTTP err", e.message); }
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
}

// ---------- 写块（判定基于区间；样式/格式同前） ----------
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink, roeRes){
  const { sheetTitle, sheetId } = await ensureToday();
  const pe = (peRes?.v==="" || peRes?.v==null) ? null : Number(peRes?.v);
  const rf = Number.isFinite(rfRes?.v) ? rfRes.v : null;

  // 口径：HS300/中概/恒生科技 用 China ERP*；SP500 用 US；日经用 Japan
  let target = erpStar;
  if(label==="沪深300" || label==="中概互联网" || label==="恒生科技") target = ERP_TARGET_CN;

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
    ["无风险利率 r_f（10Y名义）", rf ?? "", rf!=null?"真实":"兜底",
      (label==="沪深300"||label==="中概互联网"||label==="恒生科技" ? "CN 10Y":"US/JP 10Y"), rfRes?.link || "—"],
    ["目标 ERP*", (Number.isFinite(target)?target:""), (Number.isFinite(target)?"真实":"兜底"), "达摩达兰",
      erpLink || '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换（说明用，不定义卖点）","—"],
    ["买点PE上限（含ROE因子）", peBuy ?? "", (peBuy!=null)?"真实":"兜底", "买点=1/(r_f+ERP*+δ)×factor","—"],
    ["卖点PE下限（含ROE因子）", peSell ?? "", (peSell!=null)?"真实":"兜底", "卖点=1/(r_f+ERP*−δ)×factor","—"],
    ["合理PE区间（含ROE因子）", fairRange, (peBuy!=null && peSell!=null)?"真实":"兜底", "买点上限 ~ 卖点下限","—"],
    ["ROE（TTM）", roe ?? "", (roe!=null)?"真实":"兜底", "盈利能力（小数，显示为百分比）", roeRes?.link || "—"],
    ["ROE基准（可配 env.ROE_BASE）", ROE_BASE, "真实", "默认 0.12 = 12%","—"],
    ["ROE倍数因子 = ROE/ROE基准", factorDisp, (factorDisp!=="")?"真实":"兜底", "例如 16.4%/12% = 1.36","—"],
    ["说明（公式）", "见右", "真实", "买点=1/(r_f+ERP*+δ)×factor；卖点=1/(r_f+ERP*−δ)×factor；合理区间=买点~卖点","—"],
    ["判定", status, (Number.isFinite(pe) && peBuy!=null && peSell!=null)?"真实":"兜底", "基于 P/E 与区间","—"],
  ];

  const totalRows = values.length;
  const endRow = startRow + totalRows - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${endRow}`, values);

  // 样式/格式
  const base = startRow - 1;
  const pctRowsAbs = [base+2, base+3, base+4, base+5, base+9, base+10];
  const numberRowsAbs = [base+1, base+6, base+7, base+11];
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { requests: [
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
          fields:"userEnteredFormat(backgroundColor,textFormat)" } },
      { updateBorders: {
          range: { sheetId, startRowIndex: base, endRowIndex: base + totalRows, startColumnIndex:0, endColumnIndex:5 },
          top:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
          bottom:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
          left:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
          right:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } } } }
    ]}
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

  // 拉取 Value Center 表格（除 Nikkei）
  VC_CACHE = await fetchVCByTable();

  // 1) HS300（VC；r_f=CN10Y；ERP*=China）
  const rec_hs = await getVC("SH000300");
  const pe_hs = rec_hs?.pe ? { v: rec_hs.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC SH000300")` } : { v:PE_OVERRIDE_CN??"", tag:"兜底", link:"—" };
  const rf_cn  = await rfCN();
  const roe_hs = rec_hs?.roe ? { v: rec_hs.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  let r = await writeBlock(row,"沪深300", pe_hs, rf_cn, ERP_TARGET_CN, "真实", null, roe_hs);
  row = r.nextRow; const j_hs = r.judgment; const pv_hs = r.pe;

  // 2) SP500（VC；r_f=US10Y；ERP*=US）
  const rec_sp = await getVC("SP500");
  const pe_spx = rec_sp?.pe ? { v: rec_sp.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC SP500")` } : { v:PE_OVERRIDE_SPX??"", tag:"兜底", link:"—" };
  const rf_us  = await rfUS();
  const { v:erp_us_v, tag:erp_us_tag, link:erp_us_link } = await erpUS();
  const roe_spx = rec_sp?.roe ? { v: rec_sp.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  r = await writeBlock(row,"标普500", pe_spx, rf_us, erp_us_v, erp_us_tag, erp_us_link, roe_spx);
  row = r.nextRow; const j_sp = r.judgment; const pv_sp = r.pe;

  // 3) Nikkei（官方档案页；ROE 可覆写）
  const pe_nk = await peNikkei(); const rf_jp  = await rfJP(); const { v:erp_jp_v, tag:erp_jp_tag, link:erp_jp_link } = await erpJP();
  const roe_nk = (ROE_JP!=null) ? { v:ROE_JP, tag:"覆写", link:"—" } : { v:null, tag:"兜底", link:"—" };
  r = await writeBlock(row,"日经指数", pe_nk, rf_jp, erp_jp_v, erp_jp_tag, erp_jp_link, roe_nk);
  row = r.nextRow; const j_nk = r.judgment; const pv_nk = r.pe;

  // 4) 中概互联网（VC；r_f=CN10Y；ERP*=China）
  const rec_cx = await getVC("CSIH30533");
  const pe_cx = rec_cx?.pe ? { v: rec_cx.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC CSIH30533")` } : { v:PE_OVERRIDE_CXIN??"", tag:"兜底", link:"—" };
  const rf_cn2  = await rfCN(); const { v:erp_cn_v, tag:erp_cn_tag, link:erp_cn_link } = await erpCN();
  const roe_cx = rec_cx?.roe ? { v: rec_cx.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  r = await writeBlock(row,"中概互联网", pe_cx, rf_cn2, erp_cn_v, erp_cn_tag, erp_cn_link, roe_cx);
  row = r.nextRow; const j_cx = r.judgment; const pv_cx = r.pe;

  // 5) 恒生科技（VC；与中概同口径：r_f=CN10Y；ERP*=China）
  const rec_hst = await getVC("HSTECH");
  const pe_hst = rec_hst?.pe ? { v: rec_hst.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC HSTECH")` } : { v:PE_OVERRIDE_HSTECH??"", tag:"兜底", link:"—" };
  const rf_cn3 = await rfCN(); const { v:erp_hk_v, tag:erp_hk_tag, link:erp_hk_link } = await erpCN();
  const roe_hst = rec_hst?.roe ? { v: rec_hst.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  r = await writeBlock(row,"恒生科技", pe_hst, rf_cn3, erp_hk_v, erp_hk_tag, erp_hk_link, roe_hst);
  row = r.nextRow; const j_hst = r.judgment; const pv_hst = r.pe;

  console.log("[DONE]", todayStr(), {
    hs300_pe: pe_hs?.v, spx_pe: pe_spx?.v, nikkei_pe: pe_nk?.v, cxin_pe: pe_cx?.v, hstech_pe: pe_hst?.v
  });

  const lines = [
    `HS300 PE: ${pv_hs ?? "-"} → ${j_hs ?? "-"}`,
    `SPX PE: ${pv_sp ?? "-"} → ${j_sp ?? "-"}`,
    `Nikkei PE: ${pv_nk ?? "-"} → ${j_nk ?? "-"}`,
    `China Internet PE: ${pv_cx ?? "-"} → ${j_cx ?? "-"}`,
    `HSTECH PE: ${pv_hst ?? "-"} → ${j_hst ?? "-"}`
  ];
  await sendEmailIfEnabled(lines);
})();
