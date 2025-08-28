/**
 * Version History
 * V6.0.0 - 单文件模块化 + 子命令 + DRY 开关 + Notion极简同步(+Summary)
 * - 支持 --mode=test-vc/test-nifty/test-notion/test-sheet/test-mail 仅跑某模块
 * - DRY_SHEET/DRY_NOTION/DRY_MAIL 开关，开发期“看结果不落地”
 * - Notion 极简同步：Name / Valuation / AssetType / Category / Date / Summary(可选)
 * - 其它估值、写表、邮件逻辑延续现有版本
 */

import fetch from "node-fetch";
import { google } from "googleapis";
import nodemailer from "nodemailer";
import { Client as NotionClient } from "@notionhq/client";

/* =========================
   环境/标志 & CLI 子命令
   ========================= */
const DRY_SHEET  = process.env.DRY_SHEET === '1';
const DRY_NOTION = process.env.DRY_NOTION === '1';
const DRY_MAIL   = process.env.DRY_MAIL === '1';

const argv = process.argv.slice(2);
const MODE = (argv.find(a => a.startsWith('--mode=')) || '').split('=')[1] || '';

/* =========================
   全局常量
   ========================= */
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PW = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";
const DEBUG  = String(process.env.DEBUG_VERBOSE ?? "0") === "1";
const TZ     = process.env.TZ || "Asia/Shanghai";
const dbg    = (...a)=>{ if(DEBUG) console.log("[DEBUG]", ...a); };

const VC_URL = "https://danjuanfunds.com/djmodule/value-center?channel=1300100141";

// 目标指数（标签与类型）
const VC_TARGETS = {
  SH000300: { name: "沪深300", label: "HS300", category: "宽基指数", country: "CN" },
  SP500:    { name: "标普500", label: "SPX",   category: "宽基指数", country: "US" },
  CSIH30533:{ name: "中概互联50", label:"China Internet 50", category: "行业指数", country: "CN" },
  HSTECH:   { name: "恒生科技", label:"HSTECH", category: "行业指数", country: "CN" },
  NDX:      { name: "纳指100", label:"NDX", category: "宽基指数", country: "US" },
  GDAXI:    { name: "德国DAX", label:"DAX", category: "宽基指数", country: "DE" },
};

// Policy / Defaults
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.01); 
const ROE_BASE      = numOr(process.env.ROE_BASE,     0.12);

const RF_CN = numOr(process.env.RF_CN, 0.0178);
const RF_US = numOr(process.env.RF_US, 0.0425);
const RF_JP = numOr(process.env.RF_JP, 0.0100);
const RF_DE = numOr(process.env.RF_DE, 0.025);
const RF_IN = numOr(process.env.RF_IN, 0.07);

const PE_OVERRIDE_CN     = getOverride('PE_OVERRIDE_CN');
const PE_OVERRIDE_SPX    = getOverride('PE_OVERRIDE_SPX');
const PE_OVERRIDE_CXIN   = getOverride('PE_OVERRIDE_CXIN');
const PE_OVERRIDE_HSTECH = getOverride('PE_OVERRIDE_HSTECH');
const PE_OVERRIDE_NDX    = getOverride('PE_OVERRIDE_NDX');
const PE_OVERRIDE_DAX    = getOverride('PE_OVERRIDE_DAX');
function getOverride(k){ const s=(process.env[k]??"").trim(); return s?Number(s):null; }

/* =========================
   Google Sheets 初始化
   ========================= */
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if(!SPREADSHEET_ID && !DRY_SHEET){ console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL, null,
  (process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets = new google.sheets({ version:"v4", auth });
/* =========================
   Google Sheet 操作封装（顶层可用）
   ========================= */
async function ensureToday(){
  if (DRY_SHEET) return { sheetTitle: todayStr(), sheetId: 0 };
  const title = todayStr();
  const meta  = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  let sh = meta.data.sheets?.find(s => s.properties?.title === title);
  if (!sh) {
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title } } }] }
    });
    sh = { properties: add.data.replies[0].addSheet.properties };
  }
  return { sheetTitle: title, sheetId: sh.properties.sheetId };
}

async function write(range, rows){
  if (DRY_SHEET) {
    console.log("[DRY_SHEET write]", range, rows.length, "rows");
    return;
  }
  dbg("Sheet write", range, "rows:", rows.length);
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range,
    valueInputOption:"USER_ENTERED",
    requestBody:{ values: rows }
  });
}

async function clearTodaySheet(sheetTitle, sheetId){
  if (DRY_SHEET) {
    console.log("[DRY_SHEET clear]", sheetTitle);
    return;
  }
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range:`'${sheetTitle}'!A:Z`
  });
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        {
          repeatCell: {
            range:{ sheetId, startRowIndex:0, endRowIndex:2000, startColumnIndex:0, endColumnIndex:26 },
            cell:{ userEnteredFormat:{} },
            fields:"userEnteredFormat"
          }
        },
        {
          updateBorders: {
            range:{ sheetId, startRowIndex:0, endRowIndex:2000, startColumnIndex:0, endColumnIndex:26 },
            top:{style:"NONE"}, bottom:{style:"NONE"}, left:{style:"NONE"}, right:{style:"NONE"},
            innerHorizontal:{style:"NONE"}, innerVertical:{style:"NONE"}
          }
        }
      ]
    }
  });
}

async function readOneCell(range){
  if (DRY_SHEET) return ""; // DRY 模式不读表
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range });
  const v = r.data.values?.[0]?.[0];
  return (v==null || v==="") ? "" : String(v);
}

/* =========================
   Notion 初始化（极简同步）
   ========================= */
const notion = new NotionClient({ auth: process.env.NOTION_TOKEN });
const NOTION_DB_ASSETS = process.env.NOTION_DB_ASSETS;
const NOTION_SUMMARY_PAGE_ID = process.env.NOTION_SUMMARY_PAGE_ID;

// 与 Notion 数据库列名一一对应（你的库字段）
const PROP_SIMPLE = {
  Name: "Name",
  Valuation: "Valuation",
  AssetType: "AssetType",
  Category: "Category",
  Date: "Date",     // 可选
  Summary: "Summary", // Relation（可选）
  Sort: "Sort",       // Number（可选，用于固定排序）
};
let DB_PROPS = new Set();

/* =========================
   工具函数
   ========================= */

function todayStr(){
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: TZ }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
}
function numOr(v,d){ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; }

/* =========================
   股票价格获取（A股=新浪数值，其它=GoogleFinance公式）
   ========================= */

// 拆分交易所/代码
function splitTicker(ticker) {
  const [ex, code] = String(ticker || "").split(":");
  return { ex, code };
}

// 将 A 股 Ticker 转新浪代码：
// SHA:600519 -> sh600519；SHE:002027 -> sz002027
function toSinaCode(ticker) {
  const { ex, code } = splitTicker(ticker);
  if (!ex || !code) return null;
  if (ex === "SHA") return "sh" + code;
  if (ex === "SHE") return "sz" + code;
  return null;
}

// 抓取新浪接口价格（A股）
async function fetchSinaPrice(sinaCode) {
  if (!sinaCode) return null;
  const url = `http://hq.sinajs.cn/list=${sinaCode}`;
  try {
    const r = await fetch(url, { headers: { Referer: "https://finance.sina.com.cn" } });
    const txt = await r.text(); // var hq_str_sh600519="贵州茅台,1712.000,1711.000,1706.000,...";
    const m = txt.match(/"([^"]+)"/);
    if (m && m[1]) {
      const parts = m[1].split(",");
      const price = parseFloat(parts[3]); // 第 4 项通常为最新价
      if (Number.isFinite(price) && price > 0) return price;
    }
  } catch (e) {
    console.error("[SinaPrice error]", sinaCode, e?.message || e);
  }
  return null;
}

/**
 * 统一对外：返回用于写入 Sheet 的单元格数据
 * - A 股：写“数值”(API)
 * - 非 A 股：写“=GOOGLEFINANCE("<ticker>","price")”（Formula）
 */
async function fetchPriceCell(ticker) {
  const { ex } = splitTicker(ticker);

  // A 股 → 新浪接口，直接写数值
  if (ex === "SHA" || ex === "SHE") {
    const sinaCode = toSinaCode(ticker);
    const p = await fetchSinaPrice(sinaCode);
    return { value: Number.isFinite(p) ? p : "", type: "数值", source: "API" };
  }

  // 非 A 股 → GoogleFinance 公式
  return {
    value: `=GOOGLEFINANCE("${ticker}","price")`,
    type: "Formula",
    source: "GoogleFinance",
  };
}

/* =========================
   Notion 工具函数（当天覆盖 + Summary 只挂今天）
   ========================= */

// DB 已有字段集合 & Summary 实际字段名（自动探测）
let DB_PROPS = new Set();
let PROP_SUMMARY_NAME = "Summary";  // 会在 notionSelfTest 里校正为数据库里的真实名字

async function notionSelfTest(){
  if (DRY_NOTION) return console.log("[DRY_NOTION] skip selfTest");
  if (!NOTION_DB_ASSETS) {
    console.error("[Notion] 缺少 NOTION_DB_ASSETS");
    return;
  }
  try {
    const db = await notion.databases.retrieve({ database_id: NOTION_DB_ASSETS });
    const props = Object.keys(db.properties || {});
    DB_PROPS = new Set(props);

    // 自动识别 Summary 的真实列名（防止你 DB 里改了大小写/命名）
    const match = props.find(n => n.toLowerCase() === "summary");
    if (match) PROP_SUMMARY_NAME = match;

    console.log("[Notion] DB title:", db?.title?.[0]?.plain_text || "(no title)");
    console.log("[Notion] Props:", ...props);
    console.log("[Notion] Summary prop name (auto-detected):", PROP_SUMMARY_NAME);

    if (!DB_PROPS.has("Name") || !DB_PROPS.has("Date")) {
      console.warn("[Notion] 提示：数据库需有 Name(title) 和 Date(date) 字段。");
    }
    if (!DB_PROPS.has(PROP_SUMMARY_NAME)) {
      console.warn(`[Notion] 提示：未检测到 ${PROP_SUMMARY_NAME}(relation) 字段，今天的链接将不会建立。`);
    }
    if (!NOTION_SUMMARY_PAGE_ID) {
      console.warn("[Notion] NOTION_SUMMARY_PAGE_ID 为空：今天的 Summary 不会挂。");
    }
  } catch (e) {
    console.error("[Notion] retrieve error:", e?.message || e);
  }
}

/**
 * 清理同名资产的“历史 Summary”
 * 让 Dashboard 只显示今天这一条
 */
async function clearOldSummaryLinks(assetName) {
  if (!NOTION_DB_ASSETS) return;
  if (!DB_PROPS.has(PROP_SUMMARY_NAME)) return;
  try {
    const r = await notion.databases.query({
      database_id: NOTION_DB_ASSETS,
      filter: { property: "Name", title: { equals: assetName } }
    });
    for (const page of r.results || []) {
      await notion.pages.update({
        page_id: page.id,
        properties: { [PROP_SUMMARY_NAME]: { relation: [] } }
      });
    }
  } catch (e) {
    console.error("[Notion] clearOldSummaryLinks error:", e?.message || e);
  }
}

/**
 * 同名 + 同日 去重：只保留最近一条，其余归档(archived:true)
 * 返回“保留”的 pageId
 */
async function dedupeSameDay(name, dateISO) {
  if (!NOTION_DB_ASSETS || !dateISO) return;
  try {
    const res = await notion.databases.query({
      database_id: NOTION_DB_ASSETS,
      filter: {
        and: [
          { property: "Name", title: { equals: name } },
          { property: "Date", date: { equals: dateISO } }
        ]
      },
      sorts: [{ timestamp: "last_edited_time", direction: "descending" }]
    });

    const pages = res.results || [];
    if (pages.length <= 1) return pages[0]?.id;

    const keep = pages[0].id; // 保留最新
    for (let i = 1; i < pages.length; i++) {
      await notion.pages.update({ page_id: pages[i].id, archived: true });
      console.log("[Notion] archive duplicate", name, pages[i].id);
    }
    return keep;
  } catch (e) {
    console.error("[Notion] dedupeSameDay error:", e?.message || e);
  }
}

/**
 * Upsert（Name+Date 唯一）：
 * - 先做“同日去重”(归档旧的) + 清历史 Summary
 * - 只给“今天这一条”挂 Summary（需 DB 有 relation 字段、传了 summaryId）
 * - 若存在“同名+当天”记录：update；否则 create
 * - 最后再兜底一次“同日去重”
 */
async function upsertSimpleRow({ name, valuation, assetType, category, dateISO, summaryId, sort=0 }) {
  if (DRY_NOTION) return console.log("[DRY_NOTION upsert]", { name, valuation, assetType, category, dateISO, sort });
  if (!NOTION_DB_ASSETS) { console.error("[Notion] 缺少 NOTION_DB_ASSETS"); return; }

  try {
    // 1) 当日去重 → 只留一条
    await dedupeSameDay(name, dateISO);

    // 2) 清历史 Summary（确保 Dashboard 只显示今天）
    await clearOldSummaryLinks(name);

    // 3) 查“同名 + 当天”
    const q = await notion.databases.query({
      database_id: NOTION_DB_ASSETS,
      filter: {
        and: [
          { property: "Name", title: { equals: name } },
          ...(dateISO ? [{ property: "Date", date: { equals: dateISO } }] : [])
        ]
      },
      sorts: [{ timestamp: "last_edited_time", direction: "descending" }]
    });
    let pageId = q.results?.[0]?.id;

    // 4) 组装 props（只写 DB 里存在的字段，避免报错）
    const props = {};
    if (DB_PROPS.has("Name"))      props["Name"]      = { title:     [{ text: { content: name } }] };
    if (DB_PROPS.has("Valuation")) props["Valuation"] = { rich_text: [{ text: { content: valuation } }] };
    if (DB_PROPS.has("AssetType")) props["AssetType"] = { select:    { name: assetType } };
    if (DB_PROPS.has("Category"))  props["Category"]  = { select:    { name: category } };
    if (DB_PROPS.has("Date") && dateISO) props["Date"] = { date: { start: dateISO } };
    if (DB_PROPS.has("Sort"))      props["Sort"]      = { number: sort };

    // 只给“今天这一条”挂 Summary
    if (DB_PROPS.has(PROP_SUMMARY_NAME) && summaryId) {
      props[PROP_SUMMARY_NAME] = { relation: [{ id: summaryId }] };
    }

    // 5) 覆盖更新 / 新建
    if (pageId) {
      await notion.pages.update({ page_id: pageId, properties: props });
      console.log("[Notion] update", name);
    } else {
      await notion.pages.create({ parent: { database_id: NOTION_DB_ASSETS }, properties: props });
      console.log("[Notion] insert", name);
    }

    // 6) 兜底再去重一次
    await dedupeSameDay(name, dateISO);

  } catch (e) {
    console.error("[Notion] upsert error:", e?.message || e);
  }
}

/* =========================
   Value Center 抓取 (Playwright)
   ========================= */
let VC_CACHE = null;
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
async function getVC(code){
  if(!VC_CACHE){
    try { VC_CACHE = await fetchVCMapDOM(); }
    catch(e){ dbg("VC DOM err", e.message); VC_CACHE = {}; }
  }
  return VC_CACHE[code] || null;
}

/* =========================
   Nifty50 抓取 (Trendlyne)
   ========================= */
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
    return { pe: values.pe, pb: values.pb, link: `=HYPERLINK("${url}","Nifty")` };
  } finally { await br.close(); }
}

/* =========================
   Risk-free yields (Investing.com) & ERP* (Damodaran)
   ========================= */

// 通用抓取：从 Investing.com 抓 10Y，失败就用 fallback
async function rfFromInvesting(url, fallback, label, allowComma=false){
  try {
    const r = await fetch(url, { headers:{ "User-Agent": UA, "Referer":"https://www.google.com" }, timeout:12000 });
    if (r.ok) {
      const h = await r.text(); 
      let v = null;

      // 精准匹配 instrument-price-last
      const m = h.match(/instrument-price-last[^>]*>([\d.,]+)</i);
      if (m) {
        v = Number(m[1].replace(/,/g,""))/100;
      }

      if (Number.isFinite(v) && v > 0 && v < 1) {
        return { v, tag:"真实", link:`=HYPERLINK("${url}","${label}")` };
      }
    }
  } catch(_e) {}
  return { v: fallback, tag:"兜底", link:"—" };
}

/* ========= R_f / ERP 全局映射（懒初始化） ========= */
let rfP = null;
let erpP = null;

function initRfErpMaps(){
  if (!rfP) {
    rfP = {
      CN: rfCN(),
      US: rfUS(),
      JP: rfJP(),
      DE: rfDE(),
      IN: rfIN(),
    };
  }
  if (!erpP) {
    erpP = {
      CN: erpCN(),
      US: erpUS(),
      JP: erpJP(),
      DE: erpDE(),
      IN: erpIN(),
    };
  }
}

async function getRf(country){
  initRfErpMaps();
  return await rfP[country];
}

async function getErp(country){
  initRfErpMaps();
  return await erpP[country];
}

// 五个国家/地区的 r_f（10Y）
async function rfCN(){ return await rfFromInvesting("https://cn.investing.com/rates-bonds/china-10-year-bond-yield",  RF_CN, "CN 10Y"); }
async function rfUS(){ return await rfFromInvesting("https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield",     RF_US, "US 10Y"); }
async function rfJP(){ return await rfFromInvesting("https://cn.investing.com/rates-bonds/japan-10-year-bond-yield",      RF_JP, "JP 10Y"); }
async function rfDE(){ return await rfFromInvesting("https://www.investing.com/rates-bonds/germany-10-year-bond-yield",   RF_DE, "DE 10Y"); }
async function rfIN(){ return await rfFromInvesting("https://cn.investing.com/rates-bonds/india-10-year-bond-yield",      RF_IN, "IN 10Y", true); }

// ERP*（达摩达兰），失败用兜底
async function erpFromDamodaran(re){
  try{
    const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
    const r = await fetch(url, { headers:{ "User-Agent": UA }, timeout:15000 });
    if(r.ok){
      const h = await r.text();
      const row = h.split("</tr>").find(x => re.test(x)) || "";
      const plain = row.replace(/<[^>]+>/g," ");
      const nums = [...plain.matchAll(/(\d{1,2}\.\d{1,2})\s*%/g)].map(m=>Number(m[1]));
      const v = nums.find(x=>x>2 && x<10);
      if(v!=null) return { v:v/100, tag:"真实", link:`=HYPERLINK("${url}","Damodaran")` };
    }
  }catch(_e){}
  return null;
}
async function erpCN(){ return (await erpFromDamodaran(/China/i))                  || { v:0.0527, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }
async function erpUS(){ return (await erpFromDamodaran(/(United\s*States|USA)/i))  || { v:0.0433, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }
async function erpJP(){ return (await erpFromDamodaran(/Japan/i))                  || { v:0.0527, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }
async function erpDE(){ return (await erpFromDamodaran(/Germany/i))                || { v:0.0433, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }
async function erpIN(){ return (await erpFromDamodaran(/India/i))                  || { v:0.0726, tag:"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' }; }

/* =========================
   指数写块
   ========================= */
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
    ["目标 ERP*", (Number.isFinite(erpStar)?erpStar:""), (Number.isFinite(erpStar)?"真实":"兜底"), "达摩达兰", erpLink || ""],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换（说明用，不定义卖点）","—"],
    ["买点PE上限（含ROE因子）", peBuy ?? "", (peBuy!=null)?"真实":"兜底", "买点公式","—"],
    ["卖点PE下限（含ROE因子）", peSell ?? "", (peSell!=null)?"真实":"兜底", "卖点公式","—"],
    ["合理PE区间（含ROE因子）", fairRange, (peBuy!=null && peSell!=null)?"真实":"兜底", "买点上限 ~ 卖点下限","—"],
    ["ROE（TTM）", roe ?? "", roeRes?.tag || "—", "盈利能力", roeRes?.link || "—"],
    ["ROE基准", ROE_BASE, "真实", "默认 12%","—"],
    ["ROE倍数因子", factorDisp, (factorDisp!=="")?"真实":"兜底", "例: 16.4%/12%","—"],
    ["判定", status, "真实", "最终判定","—"],
  ];
  const end = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`, rows);

// === 统一显示格式：P/E 为小数，其它为百分比（DRY_SHEET 时跳过）===
if (!DRY_SHEET) {
  const requests = [];

  // 百分比显示：E/P(3)、r_f(4)、ERP*(5)、δ(6)、ROE(10)、ROE基准(11) 的 B 列
  for (const idx of [3,4,5,6,10,11]) {
    const r = (startRow - 1) + (idx - 1);
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex:r, endRowIndex:r+1, startColumnIndex:1, endColumnIndex:2 },
        cell:  { userEnteredFormat: { numberFormat: { type:"NUMBER", pattern:"0.00%" } } },
        fields:"userEnteredFormat.numberFormat"
      }
    });
  }

  // 数值两位小数：P/E(2)、买点PE(7)、卖点PE(8)、ROE倍数因子(12) 的 B 列
  for (const idx of [2,7,8,12]) {
    const r = (startRow - 1) + (idx - 1);
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex:r, endRowIndex:r+1, startColumnIndex:1, endColumnIndex:2 },
        cell:  { userEnteredFormat: { numberFormat: { type:"NUMBER", pattern:"0.00" } } },
        fields:"userEnteredFormat.numberFormat"
      }
    });
  }

  // 合理PE区间（第9行 B 列）改为文本 "0.00 ~ 0.00"
  const fairRangeText = (peBuy!=null && peSell!=null)
    ? `${peBuy.toFixed(2)} ~ ${peSell.toFixed(2)}`
    : "";
  requests.push({
    updateCells: {
      rows: [{ values: [{ userEnteredValue: { stringValue: fairRangeText } }] }],
      fields: "userEnteredValue",
      range: { sheetId,
        startRowIndex:(startRow - 1) + (9 - 1),
        endRowIndex:  (startRow - 1) + (9),
        startColumnIndex:1, endColumnIndex:2
      }
    }
  });

  // 一次提交所有格式化请求
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { requests }
  });
}
   
  return { nextRow: end + 2, judgment: status, pe, roe };
}

/* =========================
   个股：配置 & 规则 & 写块
   ========================= */

// 1) 个股配置（以后新增/修改个股，只改这里）
const STOCKS = [
  {
    label: "腾讯控股",
    ticker: "HKG:0700",
    totalShares: 9772000000,     // 股数
    fairPE: 25,                   // 合理PE（用于估值&买卖点）
    currentProfit: 220000000000,  // 当年净利（元）
    averageProfit: null,          // 周期股可填“平均净利”
    growthRate: 0.12,             // 利润增速（成长/价值股用）
    category: "成长股"            // 成长股 / 价值股 / 周期股
  },
  {
    label: "贵州茅台",
    ticker: "SHA:600519",
    totalShares: 1256197800,
    fairPE: 30,
    currentProfit: 74753000000,
    averageProfit: null,
    growthRate: 0.09,
    category: "成长股"
  },
  {
    label: "分众传媒",
    ticker: "SHE:002027",
    totalShares: 13760000000,
    fairPE: 25,
    currentProfit: 0,
    averageProfit: 4600000000,    // 46亿（周期股使用“平均净利”估值）
    growthRate: 0.00,
    category: "周期股"
  },
];

// 2) 保留：按交易所拼接 Google Finance 的价格公式（仅作为回退显示用途，不再直接使用）
function priceFormulaFromTicker(ticker){
  const [ex, code] = String(ticker||"").split(":");
  if(!ex || !code) return "";
  if(ex === "SHA") return `=getSinaPrice("sh${code}")`;                 // 上交所（已不用，保留以防将来切换）
  if(ex === "SHE") return `=GOOGLEFINANCE("SHE:${code}","price")`;      // 深交所（已不用，保留）
  return `=GOOGLEFINANCE("${ex}:${code}","price")`;                      // 其余交易所（HKG/NYSE/NASDAQ…）
}

// 3) 类别→估值/买卖点规则
const CATEGORY_RULES = {
  "周期股": (r) => ({
    fairVal: `=B${r.avgProfit}*B${r.fairPE}`,    // 合理估值=平均净利×合理PE
    buy:     `=B${r.fairVal}*0.7`,               // 买点=合理估值×70%
    sell:    `=B${r.fairVal}*1.5`,               // 卖点=合理估值×150%
    require: ["avgProfit"]
  }),
  "成长股": (r) => ({
    fairVal: `=B${r.currentProfit}*B${r.fairPE}`,
    buy:     `=MIN(B${r.fairVal}*0.7,(B${r.futureProfit}*B${r.fairPE})/2)`,
    sell:    `=MAX(B${r.currentProfit}*50,B${r.futureProfit}*B${r.fairPE}*1.5)`,
    require: ["currentProfit"]
  }),
  "价值股": (r) => ({
    fairVal: `=B${r.currentProfit}*B${r.fairPE}`,
    buy:     `=MIN(B${r.fairVal}*0.7,(B${r.futureProfit}*B${r.fairPE})/2)`,
    sell:    `=MAX(B${r.currentProfit}*50,B${r.futureProfit}*B${r.fairPE}*1.5)`,
    require: ["currentProfit"]
  }),
};

// 4) 个股写块（价格=fetchPriceCell：A股数值、非A股=GoogleFinance公式）
async function writeStockBlock(startRow, cfg) {
  const { sheetTitle, sheetId } = await ensureToday();
  const { label, ticker, totalShares, fairPE, currentProfit, averageProfit, growthRate, category } = cfg;

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
    title:         startRow,
    price:         startRow + 1,
    mc:            startRow + 2,
    shares:        startRow + 3,
    fairPE:        startRow + 4,
    currentProfit: startRow + 5,
    avgProfit:     startRow + 6,
    futureProfit:  startRow + 7,
    fairVal:       startRow + 8,
    discount:      startRow + 9,
    buy:           startRow + 10,
    sell:          startRow + 11,
    category:      startRow + 12,
    growth:        startRow + 13,
    judgment:      startRow + 14,
  };
  const f = rule(r);

  // ★ 获取价格单元格：A股=数值，其它=GoogleFinance 公式
  const priceCell = await fetchPriceCell(ticker);

  const rows = [
    ["个股", label, "Formula", "个股估值分块", `=HYPERLINK("https://www.google.com/finance/quote/${ticker}", "Google Finance")`],
    ["价格", priceCell.value, priceCell.type, "实时价格", priceCell.source], // ← 新价格行
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

  // 写入行
  await write(`'${sheetTitle}'!A${startRow}:E${startRow + rows.length - 1}`, rows);

  // 样式&格式（DRY_SHEET 时跳过）
  if (!DRY_SHEET) {
    const requests = [];

    // Header + 边框
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex:(startRow - 1), endRowIndex: startRow, startColumnIndex: 0, endColumnIndex: 5 },
        cell:  { userEnteredFormat: { backgroundColor: { red: 0.95, green: 0.95, blue: 0.95 }, textFormat: { bold: true } } },
        fields:"userEnteredFormat(backgroundColor,textFormat)"
      }
    });
    requests.push({
      updateBorders: {
        range: { sheetId, startRowIndex:(startRow - 1), endRowIndex: startRow + rows.length - 1, startColumnIndex: 0, endColumnIndex: 5 },
        top: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } },
        bottom: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } },
        left: { style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } },
        right:{ style: "SOLID", width: 1, color: { red: 0.8, green: 0.8, blue: 0.8 } }
      }
    });

    // 数值按“亿”
    const billionRows = [r.mc, r.currentProfit, r.avgProfit, r.futureProfit, r.fairVal, r.buy, r.sell].map(x=>x-1);
    for (const rIdx of billionRows) {
      requests.push({
        repeatCell: {
          range: { sheetId, startRowIndex:rIdx, endRowIndex:rIdx+1, startColumnIndex:1, endColumnIndex:2 },
          cell:  { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: `#,##0"亿"` } } },
          fields:"userEnteredFormat.numberFormat"
        }
      });
    }

    // 总股本（亿，2位小数）
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex:r.shares-1, endRowIndex:r.shares, startColumnIndex:1, endColumnIndex:2 },
        cell:  { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: `#,##0.00"亿"` } } },
        fields:"userEnteredFormat.numberFormat"
      }
    });

    // 价格（仅当数值时才设置两位小数；公式不做数值格式）
    if (priceCell.type === "数值") {
      requests.push({
        repeatCell: {
          range: { sheetId, startRowIndex:r.price-1, endRowIndex:r.price, startColumnIndex:1, endColumnIndex:2 },
          cell:  { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: `#,##0.00` } } },
          fields:"userEnteredFormat.numberFormat"
        }
      });
    }

    // 合理PE（整数）
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex:r.fairPE-1, endRowIndex:r.fairPE, startColumnIndex:1, endColumnIndex:2 },
        cell:  { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: `#,##0` } } },
        fields:"userEnteredFormat.numberFormat"
      }
    });

    // 增速（%）
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex:r.growth-1, endRowIndex:r.growth, startColumnIndex:1, endColumnIndex:2 },
        cell:  { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00%" } } },
        fields:"userEnteredFormat.numberFormat"
      }
    });

    // 折扣率（%）
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex:r.discount-1, endRowIndex:r.discount, startColumnIndex:1, endColumnIndex:2 },
        cell:  { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00%" } } },
        fields:"userEnteredFormat.numberFormat"
      }
    });

    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests }
    });
  }

  return {
    nextRow: (startRow + rows.length + 1),
    discountCellA1: `'${sheetTitle}'!B${r.discount}`,
    judgmentCellA1: `'${sheetTitle}'!B${r.judgment}`,
    nameCellA1:     `'${sheetTitle}'!B${r.title}`,
  };
}

/* =========================
   邮件（保持与你现有逻辑一致）
   ========================= */
async function sendEmailIfEnabled(lines){
  if (DRY_MAIL) { console.log("[DRY_MAIL]", lines); return; }
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

/* =========================
   摘要文案工具
   ========================= */
const roeFmt = (r) => r != null ? ` (ROE: ${(r * 100).toFixed(2)}%)` : '';
function formatIndexLine(res, label){
  return `${label} PE: ${res.pe ?? "-"}${roeFmt(res.roe)}→ ${res.judgment ?? "-"}`;
}
function formatStockLine(label, discountRaw, judgment){
  let disPct = "-";
  if (discountRaw !== "" && discountRaw != null) {
    const s = String(discountRaw).trim();
    const n = Number(s.replace(/%/g, "").replace(/,/g, ""));
    if (!Number.isNaN(n)) {
      if (/%$/.test(s))       disPct = `${n.toFixed(2)}%`;
      else if (n>0 && n<1)    disPct = `${(n*100).toFixed(2)}%`;
      else                    disPct = `${n.toFixed(2)}%`;
    }
  }
  return `${label} 折扣率: ${disPct} → ${judgment || "-"}`;
}

/* =========================
   子命令：按模块测试
   ========================= */
async function testVC(){ console.log("[TEST:VC]", await fetchVCMapDOM()); }
async function testNifty(){ console.log("[TEST:NIFTY]", await fetchNifty50()); }
async function testNotion(){
  await notionSelfTest();
  const iso = todayStr();
  await upsertSimpleRow({
    name: 'TEST Asset', valuation: 'Valuation test → 🟢 低估',
    assetType: '指数', category: '宽基指数',
    dateISO: iso, summaryId: process.env.NOTION_SUMMARY_PAGE_ID, sort: 1
  });
  console.log('[TEST:NOTION] done');
}
async function testSheet(){
  const { sheetTitle } = await ensureToday();
  await write(`'${sheetTitle}'!A1:E1`, [['仅测试写入']]);
  console.log('[TEST:SHEET] done');
}
async function testMail(){ await sendEmailIfEnabled(['这是一封测试邮件', '第二行']); console.log('[TEST:MAIL] done'); }

/* =========================
   主流程：整条流水线
   ========================= */
async function runDaily(){
  console.log("[INFO] Run start", todayStr(), "USE_PLAYWRIGHT=", USE_PW, "TZ=", TZ);

  let row=1;
  const { sheetTitle, sheetId } = await ensureToday();
  await clearTodaySheet(sheetTitle, sheetId);
  await notionSelfTest();

  // 1) 抓 Value Center
  let vcMap = {};
  if (USE_PW) {
    try { vcMap = await fetchVCMapDOM(); } catch(e){ dbg("VC DOM err", e.message); vcMap = {}; }
  }

  // --- "全市场宽基" Title ---
  await write(`'${sheetTitle}'!A${row}:E${row}`, [["全市场宽基"]]);
  if (!DRY_SHEET) {
    const titleReq = { repeatCell: { range: { sheetId, startRowIndex: row - 1, endRowIndex: row, startColumnIndex: 0, endColumnIndex: 5 }, cell: { userEnteredFormat: { backgroundColor: { red: 0.85, green: 0.85, blue: 0.85 }, textFormat: { bold: true, fontSize: 12 } } }, fields: "userEnteredFormat(backgroundColor,textFormat)" } };
    await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests: [titleReq] } });
  }
  row += 2;

  // 3) 依次写指数块 —— 使用 vcMap 覆盖（若没有就用 PE_OVERRIDE_*）
// HS300
{
  const t = VC_TARGETS.SH000300;         // { label:"HS300", country:"CN" }
  const vc = vcMap["SH000300"] || {};
  const peRes = vc.pe  ? { v: vc.pe,  tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v: PE_OVERRIDE_CN ?? "", tag:"兜底", link:"—" };
  const roeRes= vc.roe ? { v: vc.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v:"", tag:"兜底", link:"—" };
  const rfRes  = await getRf(t.country);     // ★ CN 10Y
  const erpRes = await getErp(t.country);    // ★ CN ERP
  var res_hs = await writeBlock(row, t.label, t.country, peRes, rfRes, erpRes.v, erpRes.tag, erpRes.link, roeRes);
  row = res_hs.nextRow;
}

 // SPX
{
  const t = VC_TARGETS.SP500;            // country: "US"
  const vc = vcMap["SP500"] || {};
  const peRes = vc.pe  ? { v: vc.pe,  tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v: PE_OVERRIDE_SPX ?? "", tag:"兜底", link:"—" };
  const roeRes= vc.roe ? { v: vc.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v:"", tag:"兜底", link:"—" };
  const rfRes  = await getRf(t.country);     // ★ US 10Y
  const erpRes = await getErp(t.country);    // ★ US ERP
  var res_sp = await writeBlock(row, t.label, t.country, peRes, rfRes, erpRes.v, erpRes.tag, erpRes.link, roeRes);
  row = res_sp.nextRow;
}

// NDX
{
  const t = VC_TARGETS.NDX;
  const vc = vcMap["NDX"] || {};
  const peRes = vc.pe  ? { v: vc.pe,  tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v: PE_OVERRIDE_NDX ?? "", tag:"兜底", link:"—" };
  const roeRes= vc.roe ? { v: vc.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v:"", tag:"兜底", link:"—" };
  const rfRes  = await getRf(t.country);     // ★ US 10Y
  const erpRes = await getErp(t.country);    // ★ US ERP
  var res_ndx = await writeBlock(row, t.label, t.country, peRes, rfRes, erpRes.v, erpRes.tag, erpRes.link, roeRes);
  row = res_ndx.nextRow;
}

 // Nikkei（公式）
{
  const startRow = row;

  // ★ 改用 getRf('JP') / getErp('JP')，不要再用 rf_jp_promise / erp_jp_promise
  const rfRes  = await getRf('JP');   // { v, tag, link }
  const erpRes = await getErp('JP');  // { v, tag, link }

  const peRow      = startRow + 1;
  const pbRow      = startRow + 2;
  const rfRow      = startRow + 4;
  const erpStarRow = startRow + 5;
  const deltaRow   = startRow + 6;
  const peBuyRow   = startRow + 7;
  const peSellRow  = startRow + 8;
  const roeRow     = startRow + 10;

  const nikkei_rows = [
    ["指数", "日经指数", "Formula", "宽基/行业指数估值分块", `=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave/","Nikkei")`],
    ["P/E（TTM）", `=IMPORTXML("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per", "/html/body/div[1]/div/main/section/div/div[2]/table/tbody/tr[16]/td[3]")`, "Formula", "估值来源", `=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per","Nikkei PER")`],
    ["P/B（TTM）", `=IMPORTXML("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=pbr", "/html/body/div[1]/div/main/section/div/div[2]/table/tbody/tr[16]/td[3]")`, "Formula", "估值来源", `=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=pbr","Nikkei PBR")`],
    ["E/P = 1 / P/E", `=IF(ISNUMBER(B${peRow}), 1/B${peRow}, "")`, "Formula", "盈收益率（小数，显示为百分比）", "—"],
    ["无风险利率 r_f（10Y名义）", rfRes.v,  rfRes.tag,  "JP 10Y",     rfRes.link],
    ["目标 ERP*",              erpRes.v, erpRes.tag, "达摩达兰", erpRes.link],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换（说明用，不定义卖点）", "—"],
    // 用 ROE 倍数因子计算买/卖点（与其它指数保持一致）
    ["买点PE上限（含ROE因子）", `=1/(B${rfRow}+B${erpStarRow}+B${deltaRow})*B${roeRow}/${ROE_BASE}`, "Formula", "买点=1/(r_f+ERP*+δ)×factor", "—"],
    ["卖点PE下限（含ROE因子）", `=1/(B${rfRow}+B${erpStarRow}-B${deltaRow})*B${roeRow}/${ROE_BASE}`, "Formula", "卖点=1/(r_f+ERP−δ)×factor", "—"],
    ["合理PE区间（含ROE因子）", `=IF(AND(ISNUMBER(B${peBuyRow}),ISNUMBER(B${peSellRow})), TEXT(B${peBuyRow},"0.00")&" ~ "&TEXT(B${peSellRow},"0.00"), "")`, "Formula", "买点上限 ~ 卖点下限", "—"],
    ["ROE（TTM）", `=IF(AND(ISNUMBER(B${peRow}),ISNUMBER(B${pbRow})), B${pbRow}/B${peRow}, "")`, "Formula", "盈利能力 = P/B / P/E", "—"],
    ["判定", `=IF(ISNUMBER(B${peRow}), IF(B${peRow} <= B${peBuyRow}, "🟢 低估", IF(B${peRow} >= B${peSellRow}, "🔴 高估", "🟡 持有")), "错误")`, "Formula", "基于 P/E 与区间", "—"],
  ];

  const end = startRow + nikkei_rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`, nikkei_rows);
  row = end + 2;

  // 只需要判定用于邮件文案
  var res_nikkei = { judgment: await readOneCell(`'${sheetTitle}'!B${end}`) };
}

// China Internet 50
{
  const t = VC_TARGETS.CSIH30533;
  const vc = vcMap["CSIH30533"] || {};
  const peRes = vc.pe  ? { v: vc.pe,  tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v: PE_OVERRIDE_CXIN ?? "", tag:"兜底", link:"—" };
  const roeRes= vc.roe ? { v: vc.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v:"", tag:"兜底", link:"—" };
  const rfRes  = await getRf(t.country);     // ★ CN 10Y
  const erpRes = await getErp(t.country);    // ★ CN ERP
  var res_cx = await writeBlock(row, t.label, t.country, peRes, rfRes, erpRes.v, erpRes.tag, erpRes.link, roeRes);
  row = res_cx.nextRow;
}

// HSTECH
{
  const t = VC_TARGETS.HSTECH;
  const vc = vcMap["HSTECH"] || {};
  const peRes = vc.pe  ? { v: vc.pe,  tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v: PE_OVERRIDE_HSTECH ?? "", tag:"兜底", link:"—" };
  const roeRes= vc.roe ? { v: vc.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v:"", tag:"兜底", link:"—" };
  const rfRes  = await getRf(t.country);     // ★ CN 10Y
  const erpRes = await getErp(t.country);    // ★ CN ERP
  var res_hst = await writeBlock(row, t.label, t.country, peRes, rfRes, erpRes.v, erpRes.tag, erpRes.link, roeRes);
  row = res_hst.nextRow;
}

 // DAX
{
  const t = VC_TARGETS.GDAXI;
  const vc = vcMap["GDAXI"] || {};
  const peRes = vc.pe  ? { v: vc.pe,  tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v: PE_OVERRIDE_DAX ?? "", tag:"兜底", link:"—" };
  const roeRes= vc.roe ? { v: vc.roe, tag:"真实", link:`=HYPERLINK("${VC_URL}","VC")`} : { v:"", tag:"兜底", link:"—" };
  const rfRes  = await getRf(t.country);     // ★ DE 10Y
  const erpRes = await getErp(t.country);    // ★ DE ERP
  var res_dax = await writeBlock(row, t.label, t.country, peRes, rfRes, erpRes.v, erpRes.tag, erpRes.link, roeRes);
  row = res_dax.nextRow;
}

// Nifty 50
{
  const t = { label: "Nifty 50", country: "IN" };
  const nifty = await fetchNifty50();    // { pe, pb, link }
  const peRes = { v: nifty.pe || "", tag: nifty.pe ? "真实" : "兜底", link: nifty.link };
  const roeRes= (nifty.pe && nifty.pb) ? { v: nifty.pb / nifty.pe, tag:"计算值", link: nifty.link } : { v:"", tag:"兜底", link: nifty.link };
  const rfRes  = await getRf(t.country);     // ★ IN 10Y
  const erpRes = await getErp(t.country);    // ★ IN ERP
  var res_in = await writeBlock(row, t.label, t.country, peRes, rfRes, erpRes.v, erpRes.tag, erpRes.link, roeRes);
  row = res_in.nextRow;
}

  // --- "子公司" Title ---
  await write(`'${sheetTitle}'!A${row}:E${row}`, [["子公司"]]);
  if (!DRY_SHEET) {
    const stockTitleReq = { repeatCell: { range: { sheetId, startRowIndex: row - 1, endRowIndex: row, startColumnIndex: 0, endColumnIndex: 5 }, cell: { userEnteredFormat: { backgroundColor: { red: 0.85, green: 0.85, blue: 0.85 }, textFormat: { bold: true, fontSize: 12 } } }, fields: "userEnteredFormat(backgroundColor,textFormat)" } };
    await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests: [stockTitleReq] } });
  }
  row += 2;

  // 循环渲染 STOCKS
  const stockResults = [];
  for (const s of STOCKS) {
    const res = await writeStockBlock(row, s);
    stockResults.push({ cfg: s, res });
    row = res.nextRow;
  }

  console.log("[DONE]", todayStr());

  // ====== 组装摘要行 ======
  const indexLines = [
    formatIndexLine(res_hs, "HS300"),
    formatIndexLine(res_sp, "SPX"),
    formatIndexLine(res_ndx, "NDX"),
    `Nikkei → ${res_nikkei.judgment || "-"}`,
    formatIndexLine(res_cx, "China Internet"),
    formatIndexLine(res_hst,"HSTECH"),
    formatIndexLine(res_dax,"DAX"),
    formatIndexLine(res_in, "Nifty 50"),
  ];

  const stockLines = [];
  for (const { cfg, res } of stockResults) {
    const disRaw = await readOneCell(res.discountCellA1);
    const jud    = await readOneCell(res.judgmentCellA1);
    stockLines.push( formatStockLine(cfg.label, disRaw, jud) );
  }

  const lines = [...indexLines, ...stockLines];

   // ====== Notion 极简同步（带排序 & Summary）======
  const isoDate = todayStr();
  const simpleRows = [
    { name:"沪深300",            valuation: indexLines[0], assetType:"指数", category:"宽基指数", sort: 10 },
    { name:"S&P 500",           valuation: indexLines[1], assetType:"指数", category:"宽基指数", sort: 20 },
    { name:"Nasdaq 100",        valuation: indexLines[2], assetType:"指数", category:"宽基指数", sort: 30 },
    { name:"Nikkei 225",        valuation: indexLines[3], assetType:"指数", category:"宽基指数", sort: 40 },
    { name:"China Internet 50", valuation: indexLines[4], assetType:"指数", category:"行业指数", sort: 50 },
    { name:"HSTECH",            valuation: indexLines[5], assetType:"指数", category:"行业指数", sort: 60 },
    { name:"DAX",               valuation: indexLines[6], assetType:"指数", category:"宽基指数", sort: 70 },
    { name:"Nifty 50",          valuation: indexLines[7], assetType:"指数", category:"宽基指数", sort: 80 },
  ];
  let base = 100;
  for (let i = 0; i < stockResults.length; i++) {
    const { cfg } = stockResults[i];
    simpleRows.push({
      name: cfg.label,
      valuation: stockLines[i],
      assetType: "个股",
      category: cfg.category || "成长股",
      sort: base + i,
    });
  }

  // 逐条 Upsert（同名+当天覆盖）+ 仅今天挂 Summary；随后再去重兜底
  for (const r of simpleRows) {
    await upsertSimpleRow({
      name: r.name,
      valuation: r.valuation,
      assetType: r.assetType,
      category: r.category,
      dateISO: isoDate,
      summaryId: NOTION_SUMMARY_PAGE_ID,  // ★ 只有今天挂 Summary
      sort: r.sort
    });
  }

  // ====== 邮件 ======
  await sendEmailIfEnabled(lines);
}

/* =========================
   Dispatcher（子命令入口：
   MODE 由 dev_preview.yml 的 inputs.mode 传入；
   也兼容命令行 --mode=xxx）
   ========================= */

// 既支持 Actions 注入的 MODE，也支持命令行 --mode=test-vc
const _MODE =
  process.env.MODE ||
  ((process.argv.slice(2).find(a => a.startsWith('--mode=')) || '').split('=')[1]) ||
  'full';

console.log('[INFO] MODE =', _MODE);

(async () => {
  try {
    // 只测试 VC 抓取（不会写入外部）
    if (_MODE === 'test-vc') {
      console.log('[TEST] 只测试 VC 抓取');

      const vcMap = await fetchVCMapDOM();
      console.log('[DEBUG] VC map (DOM)', vcMap);

      // 各国家 10Y
      console.log('[TEST:R_F]');
      try { console.log('CN:', await getRf('CN')); } catch (e) { console.log('CN rf error:', e?.message || e); }
      try { console.log('US:', await getRf('US')); } catch (e) { console.log('US rf error:', e?.message || e); }
      try { console.log('JP:', await getRf('JP')); } catch (e) { console.log('JP rf error:', e?.message || e); }
      try { console.log('DE:', await getRf('DE')); } catch (e) { console.log('DE rf error:', e?.message || e); }
      try { console.log('IN:', await getRf('IN')); } catch (e) { console.log('IN rf error:', e?.message || e); }

      // 指数 → 国家 → r_f
      console.log('[TEST:Index → Country → r_f]');
      for (const [code, t] of Object.entries(VC_TARGETS)) {
        try {
          const rf = await getRf(t.country);
          console.log(`${t.label} (${t.country}) → r_f=${(rf?.v * 100).toFixed(2)}%`);
        } catch (e) {
          console.log(`${t.label} (${t.country}) → rf error:`, e?.message || e);
        }
      }
      return;
    }

    // 只测试 Nifty50 抓取
    if (_MODE === 'test-nifty') {
      console.log('[TEST] 只测试 Nifty 50 抓取');
      const nifty = await fetchNifty50();
      console.log('[TEST:NIFTY]', nifty);
      return;
    }

    // 只写 Google Sheet（是否真写由 DRY_SHEET 控制）
    if (_MODE === 'test-sheet') {
      console.log('[TEST] 只测试写 Google Sheet（DRY_SHEET=0 才会真写入）');
      await runDaily();
      return;
    }

    // 只写 Notion（建议 DRY_SHEET=1, DRY_MAIL=1）
    if (_MODE === 'test-notion') {
      console.log('[TEST] 只测试写 Notion（建议 DRY_SHEET=1, DRY_MAIL=1）');
      await runDaily();
      return;
    }

    // 只发邮件（建议 DRY_MAIL=0，其他 DRY=1）
    if (_MODE === 'test-mail') {
      console.log('[TEST] 只测试邮件发送（DRY_MAIL=0 才会真发）');
      await sendEmailIfEnabled(['这是一封测试邮件', '第二行']);
      return;
    }

    // 默认：整条流水线
    await runDaily();

  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
