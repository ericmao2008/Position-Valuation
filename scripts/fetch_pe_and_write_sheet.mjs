// HS300 + S&P500 —— 两块详表；Playwright 稳定抓“顶部红圈 PE”；E/P、r_f、隐含ERP 显示为百分比；大量 [DEBUG]
import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PW  = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";
const DEBUG   = String(process.env.DEBUG_VERBOSE ?? "0") === "1";
const TZ      = process.env.TZ || "Asia/Shanghai";
const dbg     = (...a)=>{ if(DEBUG) console.log("[DEBUG]", ...a); };

const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: TZ }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr = (v,d)=>{ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; };
const strip = (h)=>h.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");

// —— 判定参数（HS300）
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.005);

// —— 兜底（小数）
const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);   // HS300 中国10Y兜底
const RF_US = numOr(process.env.RF_US,       0.0425);   // SPX   美国10Y兜底
const PE_OVERRIDE_CN  = (()=>{ const s=(process.env.PE_OVERRIDE??"").trim();      return s?Number(s):null; })();
const PE_OVERRIDE_SPX = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim();  return s?Number(s):null; })();

// —— Sheets
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
      let v=null;
      const m1=h.match(/instrument-price-last[^>]*>(\d{1,2}\.\d{1,2})</i);
      if(m1) v=Number(m1[1]);
      if(!Number.isFinite(v)){
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

// ---------------- ERP*（US：United States 行内 2%~10% 的第一个；兜底 4.33%） ----------------
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
    const candidate = pcts.find(x => x>2 && x<10) ?? 4.33;  // 2%~10% 的第一个
    return { v: candidate/100, tag:"真实", link:`=HYPERLINK("${url}","Damodaran(US)")` };
  }catch(e){ dbg("erpUS error", e.message); }
  dbg("erpUS: fallback 0.0433");
  return { v:0.0433, tag:"兜底",
    link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")' };
}

// ---------------- Danjuan：HS300走估值页+djapi；SPX走 index-detail/SP500 ----------------
async function peFromValuationPage(code, override){
  const valUrl = `https://danjuanfunds.com/dj-valuation-table-detail/${code}`;
  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      await pg.goto(valUrl, { waitUntil:"domcontentloaded" });
      // 直接同源请求 /djapi
      const apiUrl = `https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=${code}`;
      const resp = await pg.request.get(apiUrl, { headers:{ "Referer": valUrl, "User-Agent": UA }, timeout:15000 });
      dbg("HS300 /djapi status", resp.status());
      if(resp.ok()){
        const j=await resp.json();
        const v=Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm) || null;
        dbg("HS300 /djapi pe", v);
        if(Number.isFinite(v)&&v>0&&v<1000){ await br.close(); return { v, tag:"真实", link:`=HYPERLINK("${valUrl}","Danjuan")` }; }
      }
      // 正文兜底
      const text=await pg.locator('body').innerText().catch(()=> "");
      dbg("HS300 body len", text?.length || 0);
      const m=text && text.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
      const v2=m? Number(m[1]) : null;
      dbg("HS300 body regex", v2);
      await br.close();
      if(Number.isFinite(v2)&&v2>0&&v2<1000) return { v:v2, tag:"真实", link:`=HYPERLINK("${valUrl}","Danjuan")` };
    }catch(e){ dbg("HS300 PW error", e.message); }
  }
  // HTTP 源码兜底
  try{
    const r=await fetch(valUrl,{ headers:{ "User-Agent":UA }, timeout:15000 });
    dbg("HS300 HTTP status", r.status);
    if(r.ok){
      const h=await r.text();
      const text=strip(h);
      const mTop=text.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
      if(mTop){ const v=Number(mTop[1]); dbg("HS300 HTTP regex", v); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${valUrl}","Danjuan")` }; }
      const mJson=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){ const v=Number(mJson[1]); dbg("HS300 HTTP json", v); if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${valUrl}","Danjuan")` }; }
    }
  }catch(e){ dbg("HS300 HTTP err", e.message); }
  if(override!=null) return { v:override, tag:"兜底", link:`=HYPERLINK("${valUrl}","Danjuan")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${valUrl}","Danjuan")` };
}

async function peSPX(){
  // 优先 index-detail（页面顶部更稳定）
  if (USE_PW) {
    try{
      const { chromium } = await import("playwright");
      const br = await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();
      const url = "https://danjuanfunds.com/index-detail/SP500";
      await pg.goto(url, { waitUntil:"domcontentloaded" });
      await pg.waitForTimeout(3000);  // 给前端注入时间

      let text = await pg.locator("body").innerText().catch(()=> "");
      dbg("SPX index-detail body len", text?.length || 0);
      let m = text && text.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
      if(m){
        const v = Number(m[1]);
        await br.close();
        if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan SP500")` };
      }
      const v2 = await pg.evaluate(()=>{
        const re = /PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i;
        for(const el of Array.from(document.querySelectorAll("body *"))){
          const t=(el.textContent||"").trim();
          const m=t.match(re);
          if(m) return parseFloat(m[1]);
        }
        return null;
      }).catch(()=> null);
      await br.close();
      if(Number.isFinite(v2)&&v2>0&&v2<1000) return { v:v2, tag:"真实", link:`=HYPERLINK("${url}","Danjuan SP500")` };
    }catch(e){ dbg("SPX index-detail PW error", e.message); }
  }

  // 仍未命中：退到估值表页（HTTP 正则/JSON）
  const urlVal = "https://danjuanfunds.com/dj-valuation-table-detail/SP500";
  try{
    const r=await fetch(urlVal,{ headers:{ "User-Agent":UA }, timeout:15000 });
    dbg("SPX valuation HTTP status", r.status);
    if(r.ok){
      const h=await r.text();
      const text=strip(h);
      let m=text.match(/PE[\s\S]{0,80}?(\d{1,3}\.\d{1,2})/i);
      if(!m) m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(m){
        const v=Number(m[1]); dbg("SPX valuation regex/json", v);
        if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
      }
    }
  }catch(e){ dbg("SPX valuation HTTP err", e.message); }

  // 兜底（绝不返回 0）
  if(PE_OVERRIDE_SPX!=null) return { v:PE_OVERRIDE_SPX, tag:"兜底", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
  return { v:"", tag:"兜底", link:`=HYPERLINK("${urlVal}","Danjuan SP500")` };
}

async function peHS300(){ return await peFromValuationPage("SH000300", PE_OVERRIDE_CN); }

// ---------------- 单块写入（含：E/P、r_f、隐含ERP → 百分比显示） ----------------
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink){
  const { sheetTitle, sheetId } = await ensureToday();

  const pe = Number(peRes.v);                       // 抓不到时 v 为 ""，不会是 0
  const rf = Number.isFinite(rfRes.v) ? rfRes.v : null;
  const target = (label==="沪深300") ? ERP_TARGET_CN : erpStar;

  const ep = Number.isFinite(pe) ? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rf)) ? (ep - rf) : null;
  const peLimit = (Number.isFinite(rf) && Number.isFinite(target)) ? Number((1/(rf+target)).toFixed(2)) : null;

  dbg(`${label} values`, { pe, rf, target, ep, implied, peLimit, peTag: peRes.tag, rfTag: rfRes.tag });

  let status="需手动更新";
  if (implied!=null && Number.isFinite(target)) {
    if (implied >= target + 0.005) status="🟢 买点（低估）";
    else if (implied <= target - 0.005) status="🔴 卖点（高估）";
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

  // 写入
  const end = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`, rows);

  // （新增）把 E/P、r_f、隐含ERP 的 B 列设置为百分比 0.00%
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        {
          repeatCell: {
            range: {
              sheetId,
              startRowIndex: (startRow - 1) + 3, // 第 4 行（E/P）
              endRowIndex:   (startRow - 1) + 6, // 第 6 行（隐含ERP）之后不含
              startColumnIndex: 1,               // B 列
              endColumnIndex:   2
            },
            cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00%" } } },
            fields: "userEnteredFormat.numberFormat"
          }
        }
      ]
    }
  });

  return end + 2;
}

// ---------------- Main ----------------
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

  console.log("[DONE]", todayStr(), { hs300_pe: pe_hs.v, spx_pe: pe_spx.v });
})();
