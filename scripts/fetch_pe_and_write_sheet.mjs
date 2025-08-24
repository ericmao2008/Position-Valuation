// === HS300 + S&P500 ===
// 输出：按沪深300详表格式依次排列，而不是汇总总览
// 每个指数块：字段 | 数值 | 数据 | 说明 | 数据源

import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const tz = process.env.TZ || "Asia/Shanghai";
const todayStr = () => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: tz }));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr = (v, d) => { if (v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)? n : d; };

const ERP_TARGET_CN = numOr(process.env.ERP_TARGET, 0.0527);
const DELTA         = numOr(process.env.DELTA,      0.005);
const RF_CN = numOr(process.env.RF_OVERRIDE, 0.0178);
const RF_US = numOr(process.env.RF_US,       0.0425);
const PE_OVERRIDE_CN  = (()=>{ const s=(process.env.PE_OVERRIDE??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)? n:null;})();
const PE_OVERRIDE_SPX = (()=>{ const s=(process.env.PE_OVERRIDE_SPX??"").trim(); if(!s) return null; const n=Number(s); return Number.isFinite(n)? n:null;})();

// Google Sheets
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if (!SPREADSHEET_ID) { console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL, null,
  (process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version:"v4", auth });

async function ensureToday(){
  const title=todayStr();
  const meta = await sheets.spreadsheets.get({ spreadsheetId:SPREADSHEET_ID });
  let sh = meta.data.sheets?.find(s=>s.properties?.title===title);
  if(!sh){
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title }}}]}
    });
    sh = { properties: add.data.replies[0].addSheet.properties };
  }
  return { sheetTitle:title, sheetId:sh.properties.sheetId };
}
async function write(range, rows){
  await sheets.spreadsheets.values.update({
    spreadsheetId:SPREADSHEET_ID, range, valueInputOption:"USER_ENTERED", requestBody:{ values: rows }
  });
}

// 抓取函数（HS300、SPX）——略，保留你之前的Danjuan/Investing/Damodaran逻辑
// ... fetchPE_HS300()
// ... fetchPE_SPX()
// ... fetchYouzhiyouxingCN()
// ... fetchInvesting10Y_US()
// ... fetchERP_US()

// 统一写入函数（单个指数块）
async function writeBlock(startRow, label, peRes, rfRes, erpStar, erpTag, erpLink){
  const { sheetTitle } = await ensureToday();
  const pe = Number(peRes.v); const peTag = peRes.tag || (Number.isFinite(pe)?"真实":"");
  const rf = Number.isFinite(rfRes.v)? rfRes.v:0; const rfTag = rfRes.tag || (Number.isFinite(rfRes.v)?"真实":"兜底");
  const ep = Number.isFinite(pe)? 1/pe:null;
  const implied = (ep!=null && Number.isFinite(rf))? (ep-rf):null;
  const peLimit = (Number.isFinite(rf) && Number.isFinite(erpStar))? Number((1/(rf+erpStar)).toFixed(2)):null;

  let status="需手动更新";
  if(implied!=null){
    if(implied >= erpStar + DELTA) status="🟢 买点（低估）";
    else if(implied <= erpStar - DELTA) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows=[
    ["字段","数值","数据","说明","数据源"],
    ["指数",label,"真实","宽基指数估值分块", peRes.link||"—"],
    ["P/E（TTM）",Number.isFinite(pe)?pe:"",peTag,"估值来源",peRes.link||"—"],
    ["E/P = 1 / P/E",ep??"",Number.isFinite(pe)?"真实":"兜底","盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）",rf??"",rfTag,"10年期国债收益率",rfRes.link||"—"],
    ["隐含ERP = E/P − r_f",implied??"",implied!=null?"真实":"兜底","市场给予的风险补偿","—"],
    ["目标 ERP*",erpStar??"",erpTag,`达摩达兰 ${label==="标普500"?"(US)":"(CN)"}`,erpLink||"—"],
    ["容忍带 δ",DELTA,"真实","减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)",peLimit??"",peLimit!=null?"真实":"兜底","直观对照","—"],
    ["判定",status,(implied!=null)?"真实":"兜底","买点/持有/卖点/需手动","—"]
  ];
  const end=startRow+rows.length-1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`,rows);
  return end+2;
}

// Main
(async()=>{
  let row=1;
  // HS300
  const peHS=await fetchPE_HS300(); const rfCN=await fetchYouzhiyouxingCN();
  row=await writeBlock(row,"沪深300",peHS,rfCN,ERP_TARGET_CN,"真实",
    '=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")');

  // SPX
  const peSPX=await fetchPE_SPX(); const rfUS=await fetchInvesting10Y_US(); const erpUS=await fetchERP_US();
  row=await writeBlock(row,"标普500",peSPX,rfUS,erpUS.v,erpUS.tag,erpUS.link);

  console.log("[DONE]",todayStr());
})();
