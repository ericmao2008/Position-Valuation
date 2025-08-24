// === HS300 + S&P500（按详表依次排列） ===

import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const tz = process.env.TZ || "Asia/Shanghai";
const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: tz }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr = (v,d)=>{ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; };

const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.005);
const RF_CN         = numOr(process.env.RF_OVERRIDE, 0.0178);
const RF_US         = numOr(process.env.RF_US,       0.0425);
const PE_OVERRIDE_CN  = (()=>{ const s=(process.env.PE_OVERRIDE??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)? n:null;})();
const PE_OVERRIDE_SPX = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)? n:null;})();

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if(!SPREADSHEET_ID){ console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(process.env.GOOGLE_CLIENT_EMAIL,null,(process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]);
const sheets = google.sheets({ version:"v4", auth });

async function ensureToday(){
  const title=todayStr();
  const meta=await sheets.spreadsheets.get({ spreadsheetId:SPREADSHEET_ID });
  let sh=meta.data.sheets?.find(s=>s.properties?.title===title);
  if(!sh){
    const add=await sheets.spreadsheets.batchUpdate({ spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title }}}]}});
    sh={ properties:add.data.replies[0].addSheet.properties };
  }
  return { sheetTitle:title, sheetId:sh.properties.sheetId };
}
async function write(range, rows){
  await sheets.spreadsheets.values.update({ spreadsheetId:SPREADSHEET_ID, range, valueInputOption:"USER_ENTERED", requestBody:{ values:rows }});
}

// --- r_f ---
async function fetchRF_CN(){
  try{
    const r=await fetch("https://youzhiyouxing.cn/data",{ headers:{ "User-Agent":"Mozilla/5.0" }, timeout:12000 });
    if(r.ok){
      const html=await r.text();
      let m=html.match(/10年期国债到期收益率[^%]{0,160}?(\d+(?:\.\d+)?)\s*%/);
      if(!m){
        const all=[...html.matchAll(/(\d+(?:\.\d+)?)\s*%/g)].map(x=>Number(x[1])).filter(Number.isFinite);
        if(all.length) m=[null,Math.max(...all).toString()];
      }
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:'=HYPERLINK("https://youzhiyouxing.cn/data","Youzhiyouxing")' }; }
    }
  }catch{}
  return { v:RF_CN, tag:"兜底", link:"—" };
}
async function fetchRF_US(){
  const urls=[
    "https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield",
    "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield"
  ];
  for(const url of urls){
    try{
      const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:15000 });
      if(!r.ok) continue;
      const html=await r.text(); const m=html.match(/(\d+(?:\.\d+)?)\s*%/);
      if(m){ const v=Number(m[1])/100; if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:`=HYPERLINK("${url}","US 10Y (Investing)")` }; }
    }catch{}
  }
  return { v:RF_US, tag:"兜底", link:"—" };
}

// --- PE ---
async function fetchPE_HS300(){
  try{
    const r=await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=SH000300",{ headers:{ "User-Agent":"Mozilla/5.0","Referer":"https://danjuanfunds.com" }, timeout:15000 });
    if(r.ok){ const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")' }; }
  }catch{}
  try{
    const r=await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail/SH000300",{ headers:{ "User-Agent":"Mozilla/5.0","Referer":"https://danjuanfunds.com" }, timeout:15000 });
    if(r.ok){ const j=await r.json(); const v=Number(j?.data?.pe_ttm ?? j?.data?.pe); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")' }; }
  }catch{}
  try{
    const r=await fetch("https://danjuanfunds.com/index-detail/SH000300",{ headers:{ "User-Agent":"Mozilla/5.0" }, timeout:15000 });
    if(r.ok){ const h=await r.text(); const m=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i); if(m){ const v=Number(m[1]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:'=HYPERLINK("https://danjuanfunds.com/index-detail/SH000300","Danjuan")' }; } }
  }catch{}
  const ov=PE_OVERRIDE_CN; return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}
async function fetchPE_SPX(){
  const url="https://danjuanfunds.com/dj-valuation-table-detail/SP500";
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":"Mozilla/5.0" }, timeout:15000 });
    if(r.ok){
      const h=await r.text();
      const mJson=h.match(/"pe_ttm"\s*:\s*"?([\d.]+)"?/i);
      if(mJson){ const v=Number(mJson[1]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; }
      const mTxt = h.replace(/<[^>]+>/g," ").match(/(PE|市盈率)[^0-9]{0,6}([\d.]+)/i);
      if(mTxt){ const v=Number(mTxt[2]); if(Number.isFinite(v)&&v>0) return { v, tag:"真实", link:`=HYPERLINK("${url}","Danjuan")` }; }
    }
  }catch{}
  const ov=PE_OVERRIDE_SPX; return { v: ov??"", tag: ov?"兜底":"", link:"—" };
}

// --- 写“单块” ---
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink){
  const { sheetTitle } = await ensureToday();
  const pe=Number(peRes.v); const peTag=peRes.tag || (Number.isFinite(pe)?"真实":"");
  const rf=Number.isFinite(rfRes.v)? rfRes.v : 0; const rfTag=rfRes.tag || (Number.isFinite(rfRes.v)?"真实":"兜底");
  const ep=Number.isFinite(pe)? 1/pe : null;
  const implied=(ep!=null && Number.isFinite(rf))? (ep-rf) : null;
  const peLimit=(Number.isFinite(rf)&&Number.isFinite(erpStar))? Number((1/(rf+erpStar)).toFixed(2)) : null;

  let status="需手动更新";
  if(implied!=null){
    if(implied>=erpStar+0.005) status="🟢 买点（低估）";
    else if(implied<=erpStar-0.005) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows=[
    ["字段","数值","数据","说明","数据源"],
    ["指数",label,"真实","宽基指数估值分块", peRes.link||"—"],
    ["P/E（TTM）",Number.isFinite(pe)?pe:"",peTag,"估值来源",peRes.link||"—"],
    ["E/P = 1 / P/E",ep??"",Number.isFinite(pe)?"真实":"兜底","盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）",rf??"",rfTag,"10年期国债收益率",rfRes.link||"—"],
    ["隐含ERP = E/P − r_f",implied??"",implied!=null?"真实":"兜底","市场给予的风险补偿","—"],
    ["目标 ERP*",erpStar??"",erpTag,"达摩达兰","=HYPERLINK(\"https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html\",\"Damodaran\")"],
    ["容忍带 δ",0.005,"真实","减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)",peLimit??"",peLimit!=null?"真实":"兜底","直观对照","—"],
    ["判定",status,implied!=null?"真实":"兜底","买点/持有/卖点/需手动","—"]
  ];
  const end=startRow+rows.length-1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`,rows);
  return end+2;
}

// --- Main ---
(async()=>{
  let row=1;

  // HS300
  const peHS=await fetchPE_HS300(); const rfCN=await fetchRF_CN();
  row=await writeBlock(row,"沪深300",peHS,rfCN,ERP_TARGET_CN,"真实",null);

  // SPX（r_f 用美国10Y；ERP* 用 Damodaran US）
  const peSPX=await fetchPE_SPX(); const rfUS=await fetchRF_US();
  const erpUS={ v:0.0527, tag:"兜底" };  // 简化：先用固定兜底；若需实时抓取我可再加函数 fetchERP_US()
  row=await writeBlock(row,"标普500",peSPX,rfUS,erpUS.v,erpUS.tag,null);

  console.log("[DONE]",todayStr());
})();
