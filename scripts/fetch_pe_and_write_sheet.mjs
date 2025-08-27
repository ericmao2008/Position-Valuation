/**
 * Version History
 * V5.1.0 - Notion极简同步（Name/Valuation/AssetType/Category/[Date]）
 * - 仅把类似邮件的单行摘要写入 Notion
 * - 其它估值/Sheet/邮件逻辑保持 V5.0
 */

import fetch from "node-fetch";
import { google } from "googleapis";
import nodemailer from "nodemailer";
import fs from "fs";
import path from "path";

// ===== Notion（极简同步）=====
import { Client as NotionClient } from "@notionhq/client";
const notion = new NotionClient({ auth: process.env.NOTION_TOKEN });
const NOTION_DB_ASSETS = process.env.NOTION_DB_ASSETS;

// 与 Notion 数据库列名一一对应（若你改名，改这里即可）
const PROP_SIMPLE = {
  Name: "Name",
  Valuation: "Valuation",
  AssetType: "AssetType",
  Category: "Category",
  Date: "Date", // 可选：库里没有也能跑
};

// 缓存数据库可用列，避免 property_not_found
let DB_PROPS = new Set();
async function notionSelfTest(){
  if (!NOTION_DB_ASSETS || !process.env.NOTION_TOKEN) return;
  try {
    const db = await notion.databases.retrieve({ database_id: NOTION_DB_ASSETS });
    DB_PROPS = new Set(Object.keys(db.properties));
    console.log("[Notion] DB title:", db.title?.[0]?.plain_text);
    console.log("[Notion] Props:", [...DB_PROPS].join(", "));
  } catch(e) {
    console.error("[Notion] DB retrieve failed:", e?.message || e);
  }
}
const Sel = (name)=> name ? { select: { name } } : undefined;
function setIfExists(props, key, value){
  if (key && DB_PROPS.has(key) && value !== undefined) props[key] = value;
}
// Name + Date 唯一（若无 Date 列，每次新增）
async function findPageByNameDate(dbId, name, dateISO){
  if (!DB_PROPS.has(PROP_SIMPLE.Date)) return null;
  const r = await notion.databases.query({
    database_id: dbId,
    filter: { and: [
      { property: PROP_SIMPLE.Name, title: { equals: String(name) } },
      { property: PROP_SIMPLE.Date, date:  { equals: dateISO } }
    ]},
    page_size: 1
  });
  return r.results?.[0] || null;
}
// 增：让 upsertSimpleRow 支持传入 summaryId（dashboard 里那条页面的 page_id）
async function upsertSimpleRow({ name, valuation, assetType, category, dateISO, summaryId }){
  if (!NOTION_DB_ASSETS || !process.env.NOTION_TOKEN) {
    console.log("[Notion] skip: env not set"); return;
  }
  const props = {};
  props[PROP_SIMPLE.Name]      = { title:     [{ text: { content: name } }] };
  props[PROP_SIMPLE.Valuation] = { rich_text: [{ text: { content: valuation } }] };
  setIfExists(props, PROP_SIMPLE.AssetType, Sel(assetType));
  setIfExists(props, PROP_SIMPLE.Category,  Sel(category));
  setIfExists(props, PROP_SIMPLE.Date,      dateISO ? { date: { start: dateISO } } : undefined);

  // ★ 如数据库里存在 Summary 字段，并且传入了 summaryId，则建立关联
  if (summaryId && DB_PROPS.has("Summary")) {
    props["Summary"] = { relation: [{ id: summaryId }] };
  }

  try {
    // ★ 先清理旧记录上的 Summary（确保 dashboard 只指向“当天最新”）
    if (DB_PROPS.has("Summary")) {
      await clearOldSummaryLinks(name);
    }

    const exist = await findPageByNameDate(NOTION_DB_ASSETS, name, dateISO);
    if (exist) {
      await notion.pages.update({ page_id: exist.id, properties: props });
      console.log(`[Notion] updated: ${name}`);
    } else {
      await notion.pages.create({ parent: { database_id: NOTION_DB_ASSETS }, properties: props });
      console.log(`[Notion] created: ${name}`);
    }
  } catch(e){
    console.error("[Notion] upsertSimple error:", e?.message || e);
  }
}
// 清理旧的 Summary 关系：把同名资产的历史行 Summary 清空
async function clearOldSummaryLinks(assetName) {
  try {
    const r = await notion.databases.query({
      database_id: NOTION_DB_ASSETS,
      filter: { property: PROP_SIMPLE.Name, title: { equals: assetName } }
    });
    for (const page of r.results) {
      // 只清空 Summary，不动其它字段
      if (DB_PROPS.has("Summary")) {
        await notion.pages.update({
          page_id: page.id,
          properties: { Summary: { relation: [] } }
        });
      }
    }
  } catch (e) {
    console.error("[Notion] clearOldSummaryLinks:", e?.message || e);
  }
}
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
const ROE_BASE      = numOr(process.env.ROE_BASE,     0.12);

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
async function readOneCell(range){
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range });
  const v = r.data.values?.[0]?.[0];
  return (v==null || v==="") ? "" : String(v);
}

// ===== Value Center抓取（Playwright）=====
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

    if (rows.length === 0 || nameDivs.length === 0 || rows.length !== nameDivs.length) return { error: 'Mismatch' };

    for (let i = 0; i < nameDivs.length; i++) {
      const nameDivText = nameDivs[i].textContent || '';
      for (const [code, target] of Object.entries(targets)) {
        if (nameDivText.includes(target.name) || nameDivText.includes(target.code)) {
          const dataRow = rows[i];
          if (dataRow) {
            const peEl = dataRow.querySelector('.pe');
            const roeEl = dataRow.querySelector('.roe');
            const pe = toNum(peEl ? peEl.textContent : null);
            const roe = pct2d(roeEl ? roeEl.textContent : null);
            if(pe && pe > 0) out[code] = { pe, roe };
          }
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

// ===== r_f =====
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
    const m=h.match(/(\d{1,2}[.,]\d{1,4})</i); if(m) v=Number(String(m[1]).replace(',','.'))/100;
    if(!Number.isFinite(v)){ const plain=h.replace(/<[^>]+>/g," "); const near=plain.match(/(\d{1,2}[.,]\d{1,4})\s*%/); if(near) v=Number(String(near[1]).replace(',','.'))/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","IN 10Y")` };
  }}catch{} return { v:RF_IN, tag:"兜底", link:"—" }; }

// ===== ERP*（达摩达兰）=====
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

// ===== Nifty 50（Playwright）=====
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
      const m = peTitle?.textContent?.match(/of NIFTY is ([\d\.]+)/);
      if (m && m[1]) pe = parseFloat(m[1]);
      const rows = Array.from(document.querySelectorAll('tr.stock-indicator-tile-v2'));
      const pbRow = rows.find(r => (r.querySelector('th a span.stock-indicator-title')||{}).textContent?.includes('PB'));
      const el = pbRow?.querySelector('td.block_content span.fs1p5rem');
      if (el) pb = parseFloat(el.textContent.trim());
      return { pe, pb };
    });
    const peRes = (Number.isFinite(values.pe) && values.pe > 0) ? { v: values.pe, tag: "真实", link: `=HYPERLINK("${url}","Nifty PE")` } : { v: "", tag: "兜底", link: `=HYPERLINK("${url}","Nifty PE")` };
    const pbRes = (Number.isFinite(values.pb) && values.pb > 0) ? { v: values.pb, tag: "真实", link: `=HYPERLINK("${url}","Nifty PB")` } : { v: "", tag: "兜底", link: `=HYPERLINK("${url}","Nifty PB")` };
    return { peRes, pbRes };
  } finally { await br.close(); }
}

// ===== 指数写块 =====
async function writeBlock(startRow,label,country,peRes,rfRes,erpStar,erpTag,erpLink,roeRes){
  const { sheetTitle, sheetId } = await ensureToday();
  const pe = (peRes?.v==="" || peRes?.v==null) ? null : Number(peRes?.v);
  const rf = Number.isFinite(rfRes?.v) ? rfRes.v : null;
  const roe = Number.isFinite(roeRes?.v) ? roeRes.v : null;
  const ep = Number.isFinite(pe) ? 1/pe : null;
  const factor = (roe!=null && roe>0) ? (roe/ROE_BASE) : 1;
  const factorDisp = (roe!=null && roe>0) ? Number(factor.toFixed(2)) : "";
  const peBuy  = (rf!=null && erpStar!=null) ? Number((1/(rf+erpStar+DELTA)*factor).toFixed(2)) : null;
  const peSell = (rf!=null && erpStar!=null && (rf+erpStar-DELTA)>0) ? Number((1/(rf+erpStar-DELTA)*factor).toFixed(2)) : null;
  const fairRange = (peBuy!=null && peSell!=null) ? `${peBuy} ~ ${peSell}` : "";
  
  let status="需手动更新";
  if(Number.isFinite(pe) && peBuy!=null && peSell!=null){
    if (pe <= peBuy) status="🟢 低估";
    else if (pe >= peSell) status="🔴 高估";
    else status="🟡 持有";
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
  [3,4,5,6,10,11].forEach(i=>{ const r=(startRow-1)+i-1;
    requests.push({ repeatCell:{ range:{ sheetId, startRowIndex:r, endRowIndex:r+1, startColumnIndex:1, endColumnIndex:2 },
      cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } }, fields:"userEnteredFormat.numberFormat" }}); });
  [2,7,8,12].forEach(i=>{ const r=(startRow-1)+i-1;
    requests.push({ repeatCell:{ range:{ sheetId, startRowIndex:r, endRowIndex:r+1, startColumnIndex:1, endColumnIndex:2 },
      cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" }}); });
  requests.push({ repeatCell:{ range:{ sheetId, startRowIndex:(startRow-1), endRowIndex:startRow, startColumnIndex:0, endColumnIndex:5 },
    cell:{ userEnteredFormat:{ backgroundColor:{ red:0.95, green:0.95, blue:0.95 }, textFormat:{ bold:true } } }, fields:"userEnteredFormat(backgroundColor,textFormat)" }});
  requests.push({ updateBorders:{ range:{ sheetId, startRowIndex:(startRow-1), endRowIndex:end, startColumnIndex:0, endColumnIndex:5 },
    top:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
    bottom:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
    left:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } },
    right:{ style:"SOLID", width:1, color:{ red:0.8, green:0.8, blue:0.8 } } }});
  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests } });

  return { nextRow: end + 2, judgment: status, pe, roe, peBuy, peSell };
}

/* =========================
   配置化：个股数组 + 类别规则
   ========================= */
const STOCKS = [
  { label:"腾讯控股", ticker:"HKG:0700", totalShares:9772000000, fairPE:25, currentProfit:220000000000, averageProfit:null, growthRate:0.12, category:"成长股" },
  { label:"贵州茅台", ticker:"SHA:600519", totalShares:1256197800, fairPE:30, currentProfit:74753000000, averageProfit:null, growthRate:0.09, category:"成长股" },
  { label:"分众传媒", ticker:"SHE:002027", totalShares:13760000000, fairPE:25, currentProfit:0, averageProfit:4600000000, growthRate:0.00, category:"周期股" },
];

function priceFormulaFromTicker(ticker){
  const [ex, code] = String(ticker||"").split(":");
  if(!ex || !code) return "";
  if(ex === "SHA") return `=getSinaPrice("sh${code}")`;
  if(ex === "SHE") return `=GOOGLEFINANCE("SHE:${code}","price")`;
  return `=GOOGLEFINANCE("${ex}:${code}","price")`;
}

const CATEGORY_RULES = {
  "周期股": (r) => ({ fairVal:`=B${r.avgProfit}*B${r.fairPE}`, buy:`=B${r.fairVal}*0.7`, sell:`=B${r.fairVal}*1.5`, require:["avgProfit"] }),
  "成长股": (r) => ({ fairVal:`=B${r.currentProfit}*B${r.fairPE}`, buy:`=MIN(B${r.fairVal}*0.7,(B${r.futureProfit}*B${r.fairPE})/2)`, sell:`=MAX(B${r.currentProfit}*50,B${r.futureProfit}*B${r.fairPE}*1.5)`, require:["currentProfit"] }),
  "价值股": (r) => ({ fairVal:`=B${r.currentProfit}*B${r.fairPE}`, buy:`=MIN(B${r.fairVal}*0.7,(B${r.futureProfit}*B${r.fairPE})/2)`, sell:`=MAX(B${r.currentProfit}*50,B${r.futureProfit}*B${r.fairPE}*1.5)`, require:["currentProfit"] }),
};

// ===== 个股写块 =====
async function writeStockBlock(startRow, cfg) {
  const { sheetTitle, sheetId } = await ensureToday();
  const { label, ticker, totalShares, fairPE, currentProfit, averageProfit, growthRate, category } = cfg;
  const priceFormula = cfg.priceFormula ?? priceFormulaFromTicker(ticker);
  const rule = CATEGORY_RULES[category];
  if(!rule) throw new Error(`未知类别: ${category}`);
  if(rule.require){
    for(const need of rule.require){
      if(need==="avgProfit" && !(averageProfit>0)) throw new Error(`[${label}] 周期股必须提供 averageProfit`);
      if(need==="currentProfit" && !(currentProfit>0)) throw new Error(`[${label}] ${category} 必须提供 currentProfit`);
    }
  }
  const E8 = 100000000;
  const r = {
    title:startRow, price:startRow+1, mc:startRow+2, shares:startRow+3, fairPE:startRow+4,
    currentProfit:startRow+5, avgProfit:startRow+6, futureProfit:startRow+7,
    fairVal:startRow+8, discount:startRow+9, buy:startRow+10, sell:startRow+11,
    category:startRow+12, growth:startRow+13, judgment:startRow+14,
  };
  const f = rule(r);

  const rows = [
    ["个股", label, "Formula", "个股估值分块", `=HYPERLINK("https://www.google.com/finance/quote/${ticker}", "Google Finance")`],
    ["价格", priceFormula, "Formula", "实时价格", "Google Finance"],
    ["总市值", `=(B${r.price}*B${r.shares})`, "Formula", "价格 × 总股本", "—"],
    ["总股本", totalShares / E8, "Formula", "单位: 亿股", "用户提供"],
    ["合理PE", fairPE, "Fixed", `基于商业模式和增速的估算`, "—"],
    ["当年净利润", (currentProfit||0) / E8, "Fixed", "年报后需手动更新", "—"],
    ["平均净利润", (averageProfit!=null? averageProfit/E8 : ""), "Fixed", "仅“类别=周期股”时生效", "—"],
    ["3年后净利润", `=B${r.currentProfit} * (1+B${r.growth})^3`, "Formula", "当年净利润 * (1+增速)^3", "—"],
    ["合理估值", f.fairVal, "Formula", "由类别规则生成", "—"],
    ["折扣率", `=IFERROR(B${r.mc}/B${r.fairVal},"")`, "Formula", "总市值 ÷ 合理估值", "—"],
    ["买点", f.buy, "Formula", "由类别规则生成", "—"],
    ["卖点", f.sell, "Formula", "由类别规则生成", "—"],
    ["类别", category, "Fixed", "—", "—"],
    ["利润增速", growthRate, "Fixed", "用于“成长/价值股”的未来利润", "—"],
    ["判定", `=IF(ISNUMBER(B${r.mc}), IF(B${r.mc} <= B${r.buy}, "🟢 低估", IF(B${r.mc} >= B${r.sell}, "🔴 高估", "🟡 持有")), "错误")`, "Formula", "基于 总市值 与 买卖点", "—"],
  ];
  await write(`'${sheetTitle}'!A${startRow}:E${startRow + rows.length - 1}`, rows);

  const requests = [];
  requests.push({ repeatCell: { range: { sheetId, startRowIndex:(startRow - 1), endRowIndex: startRow, startColumnIndex: 0, endColumnIndex: 5 }, cell: { userEnteredFormat: { backgroundColor: { red: 0.95, green: 0.95, blue: 0.95 }, textFormat: { bold: true } } }, fields: "userEnteredFormat(backgroundColor,textFormat)" } });
  requests.push({ updateBorders: { range: { sheetId, startRowIndex:(startRow - 1), endRowIndex: startRow + rows.length - 1, startColumnIndex: 0, endColumnIndex: 5 }, top: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } }, bottom: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } }, left: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } }, right: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } } } });
  const billionRows = [r.mc, r.currentProfit, r.avgProfit, r.futureProfit, r.fairVal, r.buy, r.sell].map(x=>x-1);
  billionRows.forEach(rIdx => { requests.push({ repeatCell: { range: { sheetId, startRowIndex:rIdx, endRowIndex:rIdx+1, startColumnIndex:1, endColumnIndex:2 }, cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: `#,##0"亿"` } } }, fields: "userEnteredFormat.numberFormat" } }); });
  requests.push({ repeatCell: { range: { sheetId, startRowIndex:r.shares-1, endRowIndex:r.shares, startColumnIndex:1, endColumnIndex:2 }, cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: `#,##0.00"亿"` } } }, fields: "userEnteredFormat.numberFormat" } });
  requests.push({ repeatCell: { range: { sheetId, startRowIndex:r.price-1, endRowIndex:r.price, startColumnIndex:1, endColumnIndex:2 }, cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: `#,##0.00` } } }, fields: "userEnteredFormat.numberFormat" } });
  requests.push({ repeatCell: { range: { sheetId, startRowIndex:r.fairPE-1, endRowIndex:r.fairPE, startColumnIndex:1, endColumnIndex:2 }, cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: `#,##0` } } }, fields: "userEnteredFormat.numberFormat" } });
  requests.push({ repeatCell: { range: { sheetId, startRowIndex:r.growth-1, endRowIndex:r.growth, startColumnIndex:1, endColumnIndex:2 }, cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00%" } } }, fields: "userEnteredFormat.numberFormat" } });
  requests.push({ repeatCell: { range: { sheetId, startRowIndex:r.discount-1, endRowIndex:r.discount, startColumnIndex:1, endColumnIndex:2 }, cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00%" } } }, fields: "userEnteredFormat.numberFormat" } });
  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests } });

  return {
    nextRow: (startRow + rows.length + 1),
    discountCellA1: `'${sheetTitle}'!B${r.discount}`,
    judgmentCellA1: `'${sheetTitle}'!B${r.judgment}`,
    nameCellA1:     `'${sheetTitle}'!B${r.title}`,
  };
}

// ===== 邮件 =====
async function sendEmailIfEnabled(lines){
  const { SMTP_HOST,SMTP_PORT,SMTP_USER,SMTP_PASS,MAIL_TO,MAIL_FROM_NAME,MAIL_FROM_EMAIL,FORCE_EMAIL } = process.env;
  if(!SMTP_HOST||!SMTP_PORT||!SMTP_USER||!SMTP_PASS||!MAIL_TO){ dbg("[MAIL] skip env"); return; }
  const transporter = nodemailer.createTransport({ host:SMTP_HOST, port:Number(SMTP_PORT)===465?465:Number(SMTP_PORT), secure:Number(SMTP_PORT)===465, auth:{ user:SMTP_USER, pass:SMTP_PASS }});
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

  // Notion 自检（记录可用列，避免 property_not_found）
  await notionSelfTest();

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
  const rf_jp_promise = rfJP();
  const erp_jp_promise = erpJP();
  const rf_de_promise = rfDE();
  const erp_de_promise = erpDE();
  const nifty_promise  = fetchNifty50();
  const rf_in_promise  = rfIN();
  const erp_in_promise = erpIN();
  
  // --- "全市场宽基" Title ---
  await write(`'${sheetTitle}'!A${row}:E${row}`, [["全市场宽基"]]);
  const titleReq = { repeatCell: { range: { sheetId, startRowIndex: row - 1, endRowIndex: row, startColumnIndex: 0, endColumnIndex: 5 }, cell: { userEnteredFormat: { backgroundColor: { red: 0.85, green: 0.85, blue: 0.85 }, textFormat: { bold: true, fontSize: 12 } } }, fields: "userEnteredFormat(backgroundColor,textFormat)" } };
  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests: [titleReq] } });
  row += 2;

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

  // 4) Nikkei（公式块）
  let res_nikkei = { judgment: "-" };
  {
    const startRow = row;
    const rfRes = await rf_jp_promise;
    const erpRes = await erp_jp_promise;

    const peRow = startRow + 1;
    const pbRow = startRow + 2;
    const epRow = startRow + 3;
    const rfRow = startRow + 4;
    const erpStarRow = startRow + 5;
    const deltaRow = startRow + 6;
    const peBuyRow = startRow + 7;
    const peSellRow = startRow + 8;
    const roeRow = startRow + 10;
    const roeBaseRow = startRow + 11;
    const factorRow = startRow + 12;

    const nikkei_rows = [
      ["指数", "日经指数", "Formula", "宽基/行业指数估值分块", `=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave/","Nikkei")`],
      ["P/E（TTM）", `=IMPORTXML("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per", "/html/body/div[1]/div/main/section/div/div[2]/table/tbody/tr[16]/td[3]")`, "Formula", "估值来源", `=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per","Nikkei PER")`],
      ["P/B（TTM）", `=IMPORTXML("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=pbr", "/html/body/div[1]/div/main/section/div/div[2]/table/tbody/tr[16]/td[3]")`, "Formula", "估值来源", `=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=pbr","Nikkei PBR")`],
      ["E/P = 1 / P/E", `=IF(ISNUMBER(B${peRow}), 1/B${peRow}, "")`, "Formula", "盈收益率（小数，显示为百分比）", "—"],
      ["无风险利率 r_f（10Y名义）", rfRes.v, rfRes.tag, "JP 10Y", rfRes.link],
      ["目标 ERP*", erpRes.v, erpRes.tag, "达摩达兰", erpRes.link],
      ["容忍带 δ", DELTA, "真实", "减少频繁切换（说明用，不定义卖点）", "—"],
      ["买点PE上限（含ROE因子）", `=1/(B${rfRow}+B${erpStarRow}+B${deltaRow})*B${factorRow}`, "Formula", "买点=1/(r_f+ERP*+δ)×factor", "—"],
      ["卖点PE下限（含ROE因子）", `=1/(B${rfRow}+B${erpStarRow}-B${deltaRow})*B${factorRow}`, "Formula", "卖点=1/(r_f+ERP−δ)×factor", "—"],
      ["合理PE区间（含ROE因子）", `=IF(ISNUMBER(B${peBuyRow}), TEXT(B${peBuyRow},"0.00")&" ~ "&TEXT(B${peSellRow},"0.00"), "")`, "Formula", "买点上限 ~ 卖点下限", "—"],
      ["ROE（TTM）", `=IF(AND(ISNUMBER(B${pbRow}), ISNUMBER(B${peRow}), B${peRow}<>0), B${pbRow}/B${peRow}, "")`, "Formula", "盈利能力 = P/B / P/E", "—"],
      ["ROE基准（可配 env.ROE_BASE）", ROE_BASE, "真实", "默认 0.12 = 12%", "—"],
      ["ROE倍数因子 = ROE/ROE基准", `=IF(ISNUMBER(B${roeRow}), B${roeRow}/B${roeBaseRow}, "")`, "Formula", "例如 16.4%/12% = 1.36", "—"],
      ["说明（公式）", "见右", "真实", "买点=1/(r_f+ERP*+δ)×factor；卖点=1/(r_f+ERP*−δ)×factor；合理区间=买点~卖点", "—"],
      ["判定", `=IF(ISNUMBER(B${peRow}), IF(B${peRow} <= B${peBuyRow}, "🟢 低估", IF(B${peRow} >= B${peSellRow}, "🔴 高估", "🟡 持有")), "错误")`, "Formula", "基于 P/E 与区间", "—"],
    ];
    const end = startRow + nikkei_rows.length - 1;
    await write(`'${sheetTitle}'!A${startRow}:E${end}`, nikkei_rows);
    const requests = [];
    [4,5,6,7,11,12].forEach(i => { const r = (startRow - 1) + (i - 1);
      requests.push({ repeatCell: { range: { sheetId, startRowIndex: r, endRowIndex: r + 1, startColumnIndex: 1, endColumnIndex: 2 }, cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00%" } } }, fields: "userEnteredFormat.numberFormat" } });
    });
    [2,3,8,9,13].forEach(i => { const r = (startRow - 1) + (i - 1);
      requests.push({ repeatCell: { range: { sheetId, startRowIndex: r, endRowIndex: r + 1, startColumnIndex: 1, endColumnIndex: 2 }, cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00" } } }, fields: "userEnteredFormat.numberFormat" } });
    });
    requests.push({ repeatCell: { range: { sheetId, startRowIndex: (startRow - 1), endRowIndex: startRow, startColumnIndex: 0, endColumnIndex: 5 }, cell: { userEnteredFormat: { backgroundColor: { red: 0.95, green: 0.95, blue: 0.95 }, textFormat: { bold: true } } }, fields: "userEnteredFormat(backgroundColor,textFormat)" } });
    requests.push({ updateBorders: { range: { sheetId, startRowIndex: (startRow - 1), endRowIndex: end, startColumnIndex: 0, endColumnIndex: 5 }, top: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } }, bottom: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } }, left: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } }, right: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } } } });
    await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests } });

    const nikkeiStatusCell = `'${sheetTitle}'!B${end}`;
    res_nikkei.judgment = await readOneCell(nikkeiStatusCell);
    row = end + 2;
  }

  // 5) 中概互联50
  const erp_cn = await erp_cn_promise;
  let r_cx = vcMap["CSIH30533"];
  let pe_cx = r_cx?.pe ? { v: r_cx.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_CXIN??"", tag:"兜底", link:"—" };
  let roe_cx = r_cx?.roe ? { v: r_cx.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  let res_cx = await writeBlock(row, VC_TARGETS.CSIH30533.name, "CN", pe_cx, await rf_cn_promise, erp_cn.v, erp_cn.tag, erp_cn.link, roe_cx);
  row = res_cx.nextRow;

  // 6) 恒生科技
  let r_hst = vcMap["HSTECH"];
  let pe_hst = r_hst?.pe ? { v: r_hst.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_HSTECH??"", tag:"兜底", link:"—" };
  let roe_hst = r_hst?.roe ? { v: r_hst.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  let res_hst = await writeBlock(row, VC_TARGETS.HSTECH.name, "CN", pe_hst, await rf_cn_promise, erp_cn.v, erp_cn.tag, erp_cn.link, roe_hst);
  row = res_hst.nextRow;

  // 7) 德国DAX
  const erp_de = await erp_de_promise;
  let r_dax = vcMap["GDAXI"];
  let pe_dax = r_dax?.pe ? { v: r_dax.pe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:PE_OVERRIDE_DAX??"", tag:"兜底", link:"—" };
  let roe_dax = r_dax?.roe ? { v: r_dax.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")` } : { v:"", tag:"兜底", link:"—" };
  let res_dax = await writeBlock(row, VC_TARGETS.GDAXI.name, "DE", pe_dax, await rf_de_promise, erp_de.v, erp_de.tag, erp_de.link, roe_dax);
  row = res_dax.nextRow;

  // 8) Nifty 50
  const nifty_data = await nifty_promise;
  const pe_nifty = nifty_data.peRes;
  const pb_nifty = nifty_data.pbRes;
  if (USE_PW && (!pe_nifty.v || !pb_nifty.v)) {
    console.error("[ERROR] Scraping from Trendlyne for Nifty 50 failed. No data was returned.");
    process.exit(1);
  }
  let roe_nifty = { v: null, tag: "计算值", link: pe_nifty.link };
  if (pe_nifty && pe_nifty.v && pb_nifty && pb_nifty.v) { roe_nifty.v = pb_nifty.v / pe_nifty.v; }
  const erp_in = await erp_in_promise;
  let res_in = await writeBlock(row, "Nifty 50", "IN", pe_nifty, await rf_in_promise, erp_in.v, erp_in.tag, erp_in.link, roe_nifty);
  row = res_in.nextRow;
  
  // --- "子公司" Title ---
  await write(`'${sheetTitle}'!A${row}:E${row}`, [["子公司"]]);
  const stockTitleReq = { repeatCell: { range: { sheetId, startRowIndex: row - 1, endRowIndex: row, startColumnIndex: 0, endColumnIndex: 5 }, cell: { userEnteredFormat: { backgroundColor: { red: 0.85, green: 0.85, blue: 0.85 }, textFormat: { bold: true, fontSize: 12 } } }, fields: "userEnteredFormat(backgroundColor,textFormat)" } };
  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests: [stockTitleReq] } });
  row += 2;

  // 循环渲染 STOCKS
  const stockResults = [];
  for (const s of STOCKS) {
    const res = await writeStockBlock(row, s);
    stockResults.push({ cfg: s, res });
    row = res.nextRow;
  }

  console.log("[DONE]", todayStr());
  
  const roeFmt = (r) => r != null ? ` (ROE: ${(r * 100).toFixed(2)}%)` : '';

// ====== 组装邮件行 ======
const stockLines = [];
for (const { cfg, res } of stockResults) {
  const disRaw = await readOneCell(res.discountCellA1);
  const jud    = await readOneCell(res.judgmentCellA1);

  // 兼容 0.9788 / 97.88 / "97.88%" / 带千分位
  let disPct = "-";
  if (disRaw !== "" && disRaw != null) {
    const s = String(disRaw).trim();
    const n = Number(s.replace(/%/g, "").replace(/,/g, ""));
    if (!Number.isNaN(n)) {
      if (/%$/.test(s)) {
        disPct = `${n.toFixed(2)}%`;
      } else if (n > 0 && n < 1) {
        disPct = `${(n * 100).toFixed(2)}%`;
      } else {
        disPct = `${n.toFixed(2)}%`;
      }
    }
  }

  stockLines.push(`${cfg.label} 折扣率: ${disPct} → ${jud || "-"}`);
}

const lines = [
  `HS300 PE: ${res_hs.pe ?? "-"} ${roeFmt(res_hs.roe)}→ ${res_hs.judgment ?? "-"}`,
  `SPX PE: ${res_sp.pe ?? "-"} ${roeFmt(res_sp.roe)}→ ${res_sp.judgment ?? "-"}`,
  `NDX PE: ${res_ndx.pe ?? "-"} ${roeFmt(res_ndx.roe)}→ ${res_ndx.judgment ?? "-"}`,
  `Nikkei → ${res_nikkei.judgment || "-"}`,
  `China Internet PE: ${res_cx.pe ?? "-"} ${roeFmt(res_cx.roe)}→ ${res_cx.judgment ?? "-"}`,
  `HSTECH PE: ${res_hst.pe ?? "-"} ${roeFmt(res_hst.roe)}→ ${res_hst.judgment ?? "-"}`,
  `DAX PE: ${res_dax.pe ?? "-"} ${roeFmt(res_dax.roe)}→ ${res_dax.judgment ?? "-"}`,
  `Nifty 50 PE: ${res_in.pe ?? "-"} ${roeFmt(res_in.roe)}→ ${res_in.judgment ?? "-"}`,
  ...stockLines
];

  // ====== 写入 Notion（极简摘要：Valuation 文案） ======
  const isoDate = todayStr(); // 若库里无 Date 列，仍可正常写
  const simpleRows = [
    { name:"沪深300",     valuation: lines[0], assetType:"指数", category:"宽基指数" },
    { name:"S&P 500",     valuation: lines[1], assetType:"指数", category:"宽基指数" },
    { name:"Nasdaq 100",  valuation: lines[2], assetType:"指数", category:"宽基指数" },
    { name:"Nikkei 225",  valuation: lines[3], assetType:"指数", category:"宽基指数" },
    { name:"China Internet 50", valuation: lines[4], assetType:"指数", category:"行业指数" },
    { name:"HSTECH",      valuation: lines[5], assetType:"指数", category:"行业指数" },
    { name:"DAX",         valuation: lines[6], assetType:"指数", category:"宽基指数" },
    { name:"Nifty 50",    valuation: lines[7], assetType:"指数", category:"宽基指数" },
  ];
  // 个股
  for (let i = 0; i < stockResults.length; i++) {
    const { cfg } = stockResults[i];
    const line = lines[8 + i]; // 紧接指数之后
    simpleRows.push({
      name: cfg.label,
      valuation: line,
      assetType: "个股",
      category: cfg.category || "成长股",
    });
  }
  for (const r of simpleRows) {
    await upsertSimpleRow({ name: r.name, valuation: r.valuation, assetType: r.assetType, category: r.category, dateISO: isoDate });
  }

  // ====== 邮件 ======
  await sendEmailIfEnabled(lines);
})();
