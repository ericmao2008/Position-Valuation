import fetch from "node-fetch";
import { google } from "googleapis";

// ---------- util ----------
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PLAYWRIGHT = String(process.env.USE_PLAYWRIGHT ?? "0") === "1";
const tz = process.env.TZ || "Asia/Shanghai";
const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US",{ timeZone: tz }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr = (v,d)=>{ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)?n:d; };
const strip = h => h.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");

// ---------- params ----------
const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA = numOr(process.env.DELTA, 0.005);

// r_f 兜底
const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);
const RF_US = numOr(process.env.RF_US, 0.0425);
const RF_DE = numOr(process.env.RF_DE, 0.0230);
const RF_JP = numOr(process.env.RF_JP, 0.0100);
const RF_AU = numOr(process.env.RF_AU, 0.0420);
const RF_IN = numOr(process.env.RF_IN, 0.0710);
const RF_VN = numOr(process.env.RF_VN, 0.0280);
const RF_FALLBACK = { China:RF_CN, USA:RF_US, Germany:RF_DE, Japan:RF_JP, Australia:RF_AU, India:RF_IN, Vietnam:RF_VN };

// PE 兜底
const OV = k=>{ const s=(process.env[k]??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)&&n>0&&n<1000?n:null; };

// ---------- Sheets ----------
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if(!SPREADSHEET_ID){ console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(process.env.GOOGLE_CLIENT_EMAIL,null,(process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]);
const sheets = google.sheets({ version:"v4", auth });

async function ensureToday(){
  const title = todayStr();
  const meta = await sheets.spreadsheets.get({ spreadsheetId:SPREADSHEET_ID });
  let sh = meta.data.sheets?.find(s=>s.properties?.title===title);
  if(!sh){
    const add = await sheets.spreadsheets.batchUpdate({ spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title } } }]}});
    sh = { properties: add.data.replies[0].addSheet.properties };
  }
  return { sheetTitle:title, sheetId:sh.properties.sheetId };
}
async function write(range, rows){
  await sheets.spreadsheets.values.update({ spreadsheetId:SPREADSHEET_ID, range, valueInputOption:"USER_ENTERED", requestBody:{ values: rows } });
}
async function batch(reqs){ if(!reqs?.length) return; await sheets.spreadsheets.batchUpdate({ spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:reqs } }); }

// ---------- Investing.com 10Y ----------
const INVEST_URL = {
  China:     ["https://cn.investing.com/rates-bonds/china-10-year-bond-yield","https://www.investing.com/rates-bonds/china-10-year-bond-yield"],
  USA:       ["https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield","https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"],
  Germany:   ["https://www.investing.com/rates-bonds/germany-10-year-bond-yield","https://cn.investing.com/rates-bonds/germany-10-year-bond-yield"],
  Japan:     ["https://www.investing.com/rates-bonds/japan-10-year-bond-yield","https://cn.investing.com/rates-bonds/japan-10-year-bond-yield"],
  Australia: ["https://www.investing.com/rates-bonds/australia-10-year-bond-yield","https://cn.investing.com/rates-bonds/australia-10-year-bond-yield"],
  India:     ["https://www.investing.com/rates-bonds/india-10-year-bond-yield","https://cn.investing.com/rates-bonds/india-10-year-bond-yield"],
  Vietnam:   ["https://www.investing.com/rates-bonds/vietnam-10-year-bond-yield","https://cn.investing.com/rates-bonds/vietnam-10-year-bond-yield"],
};
async function rf(country){
  const urls = INVEST_URL[country]||[];
  for(const url of urls){
    try{
      const r = await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:15000 });
      if(!r.ok) continue;
      const html = await r.text(); const m = html.match(/(\d+(?:\.\d+)?)\s*%/);
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","Investing 10Y")` }; }
    }catch{}
  }
  return { v: RF_FALLBACK[country]??0, tag:"兜底", link:"—" };
}

// ---------- Damodaran ERP* ----------
const ERP_FALLBACK = { USA:0.0527, Germany:0.0540, Japan:0.0560, Australia:0.0520, India:0.0600, China:0.0527, Vietnam:0.0700 };
async function fetchERPMap(){
  const url="https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html";
  const map={};
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:20000 });
    if(!r.ok) throw 0;
    const html=await r.text();
    const rows=html.split(/<\/tr>/i);
    for(const row of rows){
      const t=row.replace(/<[^>]+>/g," ").trim();
      if(!t) continue;
      const mC=t.match(/^([A-Za-z .&()-]+)\s/);
      const mE=t.match(/(\d+(?:\.\d+)?)\s*%/);
      if(mC&&mE){ const c=mC[1].trim(), e=Number(mE[1])/100; if(Number.isFinite(e)) map[c]=e; }
    }
    if(map["United States"]) map["USA"]=map["United States"];
  }catch{}
  for(const k of Object.keys(ERP_FALLBACK)) if(!Number.isFinite(map[k])) map[k]=ERP_FALLBACK[k];
  return map;
}

// ---------- 指定网址的 PE 抓取器 ----------
async function pe_hs300(){
  const u1="https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=SH000300";
  const u2="https://danjuanfunds.com/djapi/index_evaluation/detail/SH000300";
  try{ const r=await fetch(u1,{ headers:{ "User-Agent":UA, "Referer":"https://danjuanfunds.com" }, timeout:15000 });
       if(r.ok){ const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")' }; } }catch{}
  try{ const r=await fetch(u2,{ headers:{ "User-Agent":UA, "Referer":"https://danjuanfunds.com" }, timeout:15000 });
       if(r.ok){ const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")' }; } }catch{}
  try{ const r=await fetch("https://danjuanfunds.com/index-detail/SH000300",{ headers:{ "User-Agent":UA }, timeout:15000 });
       if(r.ok){ const h=await r.text(); const m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i); if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")' }; } } }catch{}
  const ov=OV("PE_OVERRIDE"); return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

async function pe_spx(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/SP500";
  try{ const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
       if(r.ok){ const h=await r.text(); const m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i) || strip(h).match(/(PE|市盈率)[^0-9]{0,6}([\d.]+)/i);
                 if(m){ const v=Number(m[1]||m[2]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; } } }catch{}
  const ov=OV("PE_OVERRIDE_SPX"); return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

async function pe_ndx(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/NDX";
  try{ const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
       if(r.ok){ const h=await r.text(); const m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i) || strip(h).match(/(PE|市盈率)[^0-9]{0,6}([\d.]+)/i);
                 if(m){ const v=Number(m[1]||m[2]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; } } }catch{}
  const ov=OV("PE_OVERRIDE_NDX"); return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

async function pe_dax(){
  const url="https://finance.yahoo.com/quote/DAX/";
  try{ const r=await fetch(url,{ headers:{ "User-Agent":UA,"Accept-Language":"en-US,en" }, timeout:15000 });
       if(r.ok){ const h=await r.text(); const m=h.match(/"trailingPE"\s*:\s*{\s*"raw"\s*:\s*([\d.]+)/i) || strip(h).match(/PE\s*Ratio\s*\(TTM\)[^0-9]*([\d.]+)/i);
                 if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Yahoo Finance")` }; } } }catch{}
  const ov=OV("PE_OVERRIDE_DAX"); return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

async function pe_n225(){
  const url="https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:20000 });
    if(r.ok){
      const h=await r.text();
      // 抓取 Index Weight Basis 表：取该表最后一列的第一条数据（即最新 PER）
      const seg = h.split(/Index\s+Weight\s+Basis/i)[1] || "";
      const tbl = seg.split(/<\/table>/i)[0] || seg;
      const text = strip(tbl);
      // 行拆分后，每行最后一个数字即为该行 PER；第一行通常是最新
      const lines = text.split(/\n+/).map(s=>s.trim()).filter(Boolean);
      let candidate = null;
      for(const line of lines){
        const nums = [...line.matchAll(/(\d+(?:\.\d+)?)/g)].map(m=>Number(m[1])).filter(x=>Number.isFinite(x)&&x>0&&x<1000);
        if(nums.length){ candidate = nums[nums.length-1]; break; }
      }
      if(Number.isFinite(candidate)&&candidate>0) return { v:candidate, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER")` };
    }
  }catch{}
  const ov=OV("PE_OVERRIDE_N225"); return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

async function pe_asx200(){
  const url="https://hk.finance.yahoo.com/quote/STW.AX/";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA, "Accept-Language":"zh-HK,zh,en-US;q=0.8,en;q=0.7" }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const text=strip(h);
      const m = text.match(/市盈率[^0-9]{0,6}([\d.]+)/i) || text.match(/PE\s*Ratio\s*\(TTM\)[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Yahoo HK STW.AX")` }; }
    }
  }catch{}
  const ov=OV("PE_OVERRIDE_ASX200"); return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

async function pe_nifty50(){
  const url="https://trendlyne.com/equity/PE/NIFTY/1887/nifty-50-price-to-earning-ratios/";
  if(USE_PLAYWRIGHT){
    try{
      const { chromium }=await import("playwright");
      const b=await chromium.launch({ headless:true }); const p=await b.newPage();
      p.setDefaultNavigationTimeout(15000); p.setDefaultTimeout(12000);
      await p.goto(url,{ waitUntil:"domcontentloaded" });
      const text=await p.locator("body").innerText();
      await b.close();
      const m = text.match(/P\/?E[^0-9]*([\d.]+)/i) || text.match(/TTM[^0-9]*([\d.]+)/i);
      if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Trendlyne")` }; }
    }catch{}
  }
  const ov=OV("PE_OVERRIDE_NIFTY50"); return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

async function pe_csiH30533(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533";
  try{ const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
       if(r.ok){ const h=await r.text(); const m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i) || strip(h).match(/(PE|市盈率)[^0-9]{0,6}([\d.]+)/i);
                 if(m){ const v=Number(m[1]||m[2]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; } } }catch{}
  const ov=OV("PE_OVERRIDE_CSIH30533"); return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

// 顶部总览（包含 HS300 & CSIH30533）
const SUMMARY_INDEXES = [
  { title:"沪深300",               country:"China",     peFn: pe_hs300 },
  { title:"标普500",               country:"USA",       peFn: pe_spx },
  { title:"纳斯达克100",           country:"USA",       peFn: pe_ndx },
  { title:"德国DAX",               country:"Germany",   peFn: pe_dax },
  { title:"日经225",               country:"Japan",     peFn: pe_n225 },
  { title:"澳洲ASX200",            country:"Australia", peFn: pe_asx200 },
  { title:"印度Nifty50",           country:"India",     peFn: pe_nifty50 },
  { title:"中概互联网（CSIH30533）", country:"USA",       peFn: pe_csiH30533 },   // 特指用美国10Y
];

// 下方详表顺序：HS300 → 六海外 → CSIH30533
const DETAIL_INDEXES = [
  { title:"标普500",     country:"USA",       pe: pe_spx,      home:'=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/SP500","Danjuan")', peDesc:"蛋卷估值页（SP500）" },
  { title:"纳斯达克100", country:"USA",       pe: pe_ndx,      home:'=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/NDX","Danjuan")',   peDesc:"蛋卷估值页（NDX）" },
  { title:"德国DAX",     country:"Germany",   pe: pe_dax,      home:'=HYPERLINK("https://finance.yahoo.com/quote/DAX/","Yahoo Finance")',                peDesc:"Yahoo Finance（DAX）" },
  { title:"日经225",     country:"Japan",     pe: pe_n225,     home:'=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per","Nikkei PER")', peDesc:"Nikkei 官方 PER" },
  { title:"澳洲ASX200",  country:"Australia", pe: pe_asx200,   home:'=HYPERLINK("https://hk.finance.yahoo.com/quote/STW.AX/","Yahoo HK STW.AX")',      peDesc:"Yahoo HK（市盈率）" },
  { title:"印度Nifty50", country:"India",     pe: pe_nifty50,  home:'=HYPERLINK("https://trendlyne.com/equity/PE/NIFTY/1887/nifty-50-price-to-earning-ratios/","Trendlyne")', peDesc:"Trendlyne（Nifty50 PE）" },
  { title:"中概互联网（CSIH30533）", country:"USA",       pe: pe_csiH30533, home:'=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533","Danjuan")', peDesc:"蛋卷估值页（CSIH30533）" },
];

// 顶部总览
async function writeGlobalSummary(erpMap){
  const { sheetTitle, sheetId } = await ensureToday();
  const rows = [["指数名称","当前PE","估值水平","备注"]];
  for(const it of SUMMARY_INDEXES){
    const peRes = await it.peFn(); const pe = Number(peRes.v);
    const { v:rfV } = await rf(it.country);
    const erpStar = erpMap[it.country];
    let level="—", note="";
    if(Number.isFinite(pe) && Number.isFinite(rfV) && Number.isFinite(erpStar)){
      const implied = 1/pe - rfV;
      level = implied >= erpStar + DELTA ? "🟢 低估" : implied <= erpStar - DELTA ? "🔴 高估" : "🟡 合理";
    }else{
      if(!Number.isFinite(pe)) note="（PE待接入/兜底）";
      else if(!Number.isFinite(rfV)) note="（r_f缺失）";
      else if(!Number.isFinite(erpStar)) note="（ERP*缺失）";
    }
    rows.push([it.title, Number.isFinite(pe)? pe : "", level, note]);
  }
  await write(`'${sheetTitle}'!A1:D${rows.length}`, rows);
  await batch([
    { repeatCell:{ range:{ sheetId, startRowIndex:0, endRowIndex:1, startColumnIndex:0, endColumnIndex:4 },
      cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
      fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"} }
  ]);
  return rows.length + 1;
}

// HS300 详表
async function writeHS300Block(startRow){
  const { sheetTitle, sheetId } = await ensureToday();
  const peRes = await pe_hs300(); const pe = Number(peRes.v); const peTag = peRes.tag || (Number.isFinite(pe)?"真实":"");
  const { v:rfCN, tag:rfTag, link:rfLink } = await rf("China");
  const ep = Number.isFinite(pe)? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rfCN)) ? (ep - rfCN) : null;
  const peLimit = Number.isFinite(rfCN)? Number((1/(rfCN + ERP_TARGET_CN)).toFixed(2)) : null;
  let status="需手动更新";
  if(implied!=null){
    if(implied >= ERP_TARGET_CN + DELTA) status="🟢 买点（低估）";
    else if(implied <= ERP_TARGET_CN - DELTA) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }
  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数","沪深300","真实","宽基指数估值分块", '=HYPERLINK("https://www.csindex.com.cn/zh-CN/indices/index-detail/000300","中证指数有限公司")'],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peTag, "蛋卷 index-detail（JSON→HTML）", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rfCN ?? "", rfTag, "Investing.com 中国10Y", rfLink],
    ["隐含ERP = E/P − r_f", implied ?? "", implied!=null?"真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", ERP_TARGET_CN, "真实", "建议参考达摩达兰", '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit ?? "", peLimit!=null?"真实":"兜底", "直观对照","—"],
    ["判定", status, implied!=null?"真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];
  const endRow = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${endRow}`, rows);
  return endRow + 2;
}

// 海外分块（按顺序：SPX→NDX→DAX→N225→ASX200→Nifty50→CSIH30533）
async function writeDetailBlock(startRow, cfg, erpMap){
  const { sheetTitle } = await ensureToday();
  const peRes = await cfg.pe(); const pe = Number(peRes.v);
  const rfRes = await rf(cfg.country); const rfV = rfRes.v;
  const erpStar = erpMap[cfg.country];
  const ep = Number.isFinite(pe)? 1/pe : null;
  const implied = (ep!=null && Number.isFinite(rfV))? (ep - rfV) : null;
  let status="需手动更新";
  if(implied!=null && Number.isFinite(erpStar)){
    if(implied >= erpStar + DELTA) status="🟢 买点（低估）";
    else if(implied <= erpStar - DELTA) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }
  const rows = [
    ["字段","数值","数据","说明","数据源"],
    ["指数", cfg.title, "真实", "宽基指数估值分块", cfg.home || "—"],
    ["P/E（TTM）", Number.isFinite(pe)? pe:"", peRes.tag || (Number.isFinite(pe)?"真实":""), cfg.peDesc || "—", peRes.link || "—"],
    ["E/P = 1 / P/E", ep ?? "", Number.isFinite(pe)?"真实":"兜底", "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rfV ?? "", rfRes.tag || (rfV!=null?"真实":"兜底"), "Investing.com 10Y", rfRes.link || "—"],
    ["隐含ERP = E/P − r_f", implied ?? "", implied!=null?"真实":"兜底", "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", Number.isFinite(erpStar)? erpStar:"", Number.isFinite(erpStar)?"真实":"兜底", "达摩达兰国家表", '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ", DELTA, "真实", "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", (Number.isFinite(rfV)&&Number.isFinite(erpStar))? Number((1/(rfV+erpStar)).toFixed(2)):"", (Number.isFinite(rfV)&&Number.isFinite(erpStar))?"真实":"兜底", "直观对照","—"],
    ["判定", status, (implied!=null && Number.isFinite(erpStar))?"真实":"兜底", "买点/持有/卖点/需手动","—"],
  ];
  const endRow = startRow + rows.length - 1;
  await write(`'${sheetTitle}'!A${startRow}:E${endRow}`, rows);
  return endRow + 2;
}

async function main(){
  const erpMap = await fetchERPMap();
  const nextRow = await writeGlobalSummary(erpMap);          // 顶部总览（包含 HS300 & CSIH30533）
  let row = await writeHS300Block(nextRow);                   // HS300 详表
  for(const cfg of [
    { title:"标普500",     country:"USA",       pe:pe_spx,      home:'=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/SP500","Danjuan")', peDesc:"蛋卷估值页（SP500）" },
    { title:"纳斯达克100", country:"USA",       pe:pe_ndx,      home:'=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/NDX","Danjuan")',   peDesc:"蛋卷估值页（NDX）" },
    { title:"德国DAX",     country:"Germany",   pe:pe_dax,      home:'=HYPERLINK("https://finance.yahoo.com/quote/DAX/","Yahoo Finance")',                peDesc:"Yahoo Finance（DAX）" },
    { title:"日经225",     country:"Japan",     pe:pe_n225,     home:'=HYPERLINK("https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per","Nikkei PER")', peDesc:"Nikkei 官方 PER" },
    { title:"澳洲ASX200",  country:"Australia", pe:pe_asx200,   home:'=HYPERLINK("https://hk.finance.yahoo.com/quote/STW.AX/","Yahoo HK STW.AX")',      peDesc:"Yahoo HK（市盈率）" },
    { title:"印度Nifty50", country:"India",     pe:pe_nifty50,  home:'=HYPERLINK("https://trendlyne.com/equity/PE/NIFTY/1887/nifty-50-price-to-earning-ratios/","Trendlyne")', peDesc:"Trendlyne（Nifty50 PE）" },
    { title:"中概互联网（CSIH30533）", country:"USA",       pe:pe_csiH30533, home:'=HYPERLINK("https://danjuanfunds.com/dj-valuation-table-detail/CSIH30533","Danjuan")', peDesc:"蛋卷估值页（CSIH30533）" },
  ]){
    row = await writeDetailBlock(row, cfg, erpMap);
  }
  console.log("[DONE]", todayStr());
}

// 顶部总览（包含 HS300 & CSIH30533）
async function writeGlobalSummary(erpMap){
  const { sheetTitle, sheetId } = await ensureToday();
  const rows = [["指数名称","当前PE","估值水平","备注"]];
  for(const it of [
    { title:"沪深300",               country:"China",     peFn: pe_hs300 },
    { title:"标普500",               country:"USA",       peFn: pe_spx },
    { title:"纳斯达克100",           country:"USA",       peFn: pe_ndx },
    { title:"德国DAX",               country:"Germany",   peFn: pe_dax },
    { title:"日经225",               country:"Japan",     peFn: pe_n225 },
    { title:"澳洲ASX200",            country:"Australia", peFn: pe_asx200 },
    { title:"印度Nifty50",           country:"India",     peFn: pe_nifty50 },
    { title:"中概互联网（CSIH30533）", country:"USA",       peFn: pe_csiH30533 },
  ]){
    const peRes = await it.peFn(); const pe = Number(peRes.v);
    const { v:rfV } = await rf(it.country);
    const erpStar = erpMap[it.country];
    let level="—", note="";
    if(Number.isFinite(pe) && Number.isFinite(rfV) && Number.isFinite(erpStar)){
      const implied = 1/pe - rfV;
      level = implied >= erpStar + DELTA ? "🟢 低估" : implied <= erpStar - DELTA ? "🔴 高估" : "🟡 合理";
    }else{
      if(!Number.isFinite(pe)) note="（PE待接入/兜底）";
      else if(!Number.isFinite(rfV)) note="（r_f缺失）";
      else if(!Number.isFinite(erpStar)) note="（ERP*缺失）";
    }
    rows.push([it.title, Number.isFinite(pe)? pe:"", level, note]);
  }
  await write(`'${sheetTitle}'!A1:D${rows.length}`, rows);
  await batch([{ repeatCell:{ range:{ sheetId, startRowIndex:0, endRowIndex:1, startColumnIndex:0, endColumnIndex:4 },
    cell:{ userEnteredFormat:{ backgroundColor:{ red:0.949, green:0.957, blue:0.969 }, textFormat:{ bold:true }, horizontalAlignment:"CENTER" } },
    fields:"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)" } }]);
  return rows.length + 1;
}

main().catch(e=>{ console.error(e); process.exit(1); });
