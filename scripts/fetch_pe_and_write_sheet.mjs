/**
 * Version History
 * V2.7.10
 *  - Value Center：Playwright 读取所有 <script> 文本，优先从页面注入的 JSON/NUXT 数据按 index_code 提取 pe_ttm/roe；
 *    若未命中再 DOM 兜底（锚点行：PE=第3列、ROE=第8列）。仅 HS300/SP500/CSIH30533/HSTECH 使用 VC。
 *  - 口径：HS300/CSIH/HSTECH → CN10Y + China ERP*；SP500 → US10Y + US ERP*；Nikkei → 官方档案页 PER（ROE_JP 可覆写）
 *  - 判定：基于 P/E 与 [买点, 卖点]；邮件正文包含判定；DEBUG 保留
 */

import fetch from "node-fetch";
import { google } from "googleapis";
import nodemailer from "nodemailer";

const UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const USE_PW=String(process.env.USE_PLAYWRIGHT??"0")==="1";
const DEBUG=String(process.env.DEBUG_VERBOSE??"0")==="1";
const TZ=process.env.TZ||"Asia/Shanghai";
const dbg=(...a)=>{ if(DEBUG) console.log("[DEBUG]",...a); };

const VC_URL="https://danjuanfunds.com/djmodule/value-center?channel=1300100141";
const VC_CODE_LINK={
  SH000300:"/dj-valuation-table-detail/SH000300",
  SP500:"/dj-valuation-table-detail/SP500",
  CSIH30533:"/dj-valuation-table-detail/CSIH30533",
  HSTECH:"/dj-valuation-table-detail/HSTECH",
};

const todayStr=()=>{
  const now=new Date(new Date().toLocaleString("en-US",{timeZone:TZ}));
  return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
};
const numOr=(v,d)=>{ if(v==null) return d; const s=String(v).trim(); if(!s) return d; const n=Number(s); return Number.isFinite(n)?n:d; };
const strip=h=>h.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"").replace(/<[^>]+>/g," ");

const ERP_TARGET_CN=numOr(process.env.ERP_TARGET,0.0527);
const DELTA=numOr(process.env.DELTA,0.005);
const ROE_BASE=numOr(process.env.ROE_BASE,0.12);

const RF_CN=numOr(process.env.RF_OVERRIDE,0.0178);
const RF_US=numOr(process.env.RF_US,0.0425);
const RF_JP=numOr(process.env.RF_JP,0.0100);

const PE_OVERRIDE_CN=(()=>{const s=(process.env.PE_OVERRIDE??"").trim();return s?Number(s):null;})();
const PE_OVERRIDE_SPX=(()=>{const s=(process.env.PE_OVERRIDE_SPX??"").trim();return s?Number(s):null;})();
const PE_OVERRIDE_CXIN=(()=>{const s=(process.env.PE_OVERRIDE_CXIN??"").trim();return s?Number(s):null;})();
const PE_OVERRIDE_HSTECH=(()=>{const s=(process.env.PE_OVERRIDE_HSTECH??"").trim();return s?Number(s):null;})();
const ROE_JP=numOr(process.env.ROE_JP,null);

const SPREADSHEET_ID=process.env.SPREADSHEET_ID;
if(!SPREADSHEET_ID){ console.error("缺少 SPREADSHEET_ID"); process.exit(1); }
const auth=new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL,null,
  (process.env.GOOGLE_PRIVATE_KEY||"").replace(/\\n/g,"\n"),
  ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
);
const sheets=google.sheets({version:"v4",auth});

async function ensureToday(){
  const title=todayStr();
  const meta=await sheets.spreadsheets.get({ spreadsheetId:SPREADSHEET_ID });
  let sh=meta.data.sheets?.find(s=>s.properties?.title===title);
  if(!sh){
    const add=await sheets.spreadsheets.batchUpdate({ spreadsheetId:SPREADSHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title }}}] }});
    sh={ properties:add.data.replies[0].addSheet.properties };
  }
  return { sheetTitle:title, sheetId:sh.properties.sheetId };
}
async function write(range,rows){
  dbg("Sheet write",range,"rows:",rows.length);
  await sheets.spreadsheets.values.update({ spreadsheetId:SPREADSHEET_ID, range, valueInputOption:"USER_ENTERED", requestBody:{ values:rows }});
}
async function clearTodaySheet(sheetTitle,sheetId){
  await sheets.spreadsheets.values.clear({ spreadsheetId:SPREADSHEET_ID, range:`'${sheetTitle}'!A:Z` });
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId:SPREADSHEET_ID,
    requestBody:{ requests:[
      { repeatCell:{ range:{ sheetId, startRowIndex:0,endRowIndex:2000,startColumnIndex:0,endColumnIndex:26 }, cell:{ userEnteredFormat:{} }, fields:"userEnteredFormat" } },
      { updateBorders:{ range:{ sheetId, startRowIndex:0,endRowIndex:2000,startColumnIndex:0,endColumnIndex:26 },
        top:{style:"NONE"},bottom:{style:"NONE"},left:{style:"NONE"},right:{style:"NONE"},
        innerHorizontal:{style:"NONE"},innerVertical:{style:"NONE"} } }
    ]}
  });
}

/** Value Center：从 <script> JSON/NUXT 数据解析；失败再 DOM 兜底 */
async function fetchVCMapPW(){
  const { chromium }=await import("playwright");
  const br=await chromium.launch({ headless:true, args:['--disable-blink-features=AutomationControlled'] });
  const ctx=await br.newContext({ userAgent:UA, locale:'zh-CN', timezoneId:TZ });
  const pg=await ctx.newPage();
  await pg.goto(VC_URL,{ waitUntil:'domcontentloaded' });
  // 等待任意目标锚点或脚本加载
  await Promise.race([
    pg.waitForSelector('script', { timeout: 6000 }),
    ...Object.values(VC_CODE_LINK).map(h=>pg.waitForSelector(`a[href*="${h}"]`,{ timeout:6000 }).catch(()=>{}))
  ]).catch(()=>{});
  await pg.waitForTimeout(600);

  const data = await pg.evaluate((codes,links)=>{
    const out={}; const want=Object.keys(codes);
    const grab=(t)=>{
      // 优先：严格 index_code + pe_ttm + roe
      for(const code of want){
        let ok=false;
        // JSON 块中有 index_code":"CODE"
        const re=new RegExp(`"index_code"\\s*:\\s*"${code}"[\\s\\S]{0,600}?"pe_ttm"\\s*:\\s*"?([\\d.]+)"?[\\s\\S]{0,500}?"roe"\\s*:\\s*"?([\\d.]+)"?`,"i");
        const m=t.match(re);
        if(m){
          const pe=parseFloat(m[1]); const roeRaw=parseFloat(m[2]);
          if(Number.isFinite(pe)&&pe>0&&pe<1000){
            const roe = Number.isFinite(roeRaw) ? (roeRaw>1? roeRaw/100 : roeRaw) : null;
            out[code]={ pe, roe:(roe>0&&roe<1)?roe:null }; ok=true;
          }
        }
        if(ok) continue;
        // 次选：仅 pe_ttm，缺 roe
        const re2=new RegExp(`"index_code"\\s*:\\s*"${code}"[\\s\\S]{0,600}?"pe_ttm"\\s*:\\s*"?([\\d.]+)"?`,"i");
        const m2=t.match(re2);
        if(m2){
          const pe=parseFloat(m2[1]);
          if(Number.isFinite(pe)&&pe>0&&pe<1000){ out[code]={ pe, roe:null }; }
        }
      }
    };
    // 1) 所有 script 文本
    Array.from(document.querySelectorAll('script')).forEach(s=>{ const txt=s.textContent||""; if(txt.length) grab(txt); });

    // 2) 如果某些 code 仍缺，DOM 兜底：锚点行 → 第3/8列
    const toNum=s=>{ const x=parseFloat(String(s).replace(/,/g,"").trim()); return Number.isFinite(x)?x:null; };
    const pct2d=s=>{ const m=String(s||"").match(/(-?\\d+(?:\\.\\d+)?)\\s*%/); if(!m) return null; const v=parseFloat(m[1])/100; return (v>0&&v<1)?v:null; };

    for(const code of want){
      if(out[code]) continue;
      const href = links[code];
      const a = document.querySelector(`a[href*="${href}"]`) || Array.from(document.querySelectorAll('a')).find(x=> (x.getAttribute('href')||"").includes(code));
      if(!a) continue;
      let tr=a.closest("tr");
      if(!tr){
        // 有些页面用 div 行，向上找含多个子块的容器
        let p=a.parentElement; let depth=0;
        while(p && depth<6 && !(p.querySelectorAll('td').length>=8 || p.querySelectorAll('div').length>=8)){ p=p.parentElement; depth++; }
        tr=p;
      }
      if(!tr) continue;
      const tds = Array.from(tr.querySelectorAll("td")).map(td=> td.innerText.trim());
      if(tds.length>=8){
        const pe = toNum(tds[2]);
        const roe= pct2d(tds[7]);
        if(pe && pe>0 && pe<1000){ out[code]={ pe, roe:(roe&&roe>0&&roe<1)?roe:null }; }
      }else{
        // 最后退：在容器文本里找小数与百分比（不强求）
        const txt=(tr.textContent||"").replace(/\\s+/g," ");
        const m=txt.match(/(\\d{1,3}\\.\\d{1,2})/); // 取第一个小数作 PE（弱兜底）
        const p=txt.match(/(\\d{1,2}(?:\\.\\d{1,2})?)\\s*%/); // 百分比作 ROE
        const pe=m?parseFloat(m[1]):null;
        const roe=p?parseFloat(p[1])/100:null;
        if(pe && pe>0 && pe<1000){ out[code]={ pe, roe:(roe&&roe>0&&roe<1)?roe:null }; }
      }
    }
    return out;
  }, VC_CODE_LINK, VC_CODE_LINK);

  await br.close(); // 双保险
  dbg("VC map from scripts/DOM", data);
  return data||{};
}

let VC_CACHE=null;
async function getVC(code){
  if(!VC_CACHE){
    try{ VC_CACHE=await fetchVCMapPW(); }catch(e){ dbg("VC fetch err",e.message); VC_CACHE={}; }
  }
  return VC_CACHE[code]||null;
}

// ===== r_f / ERP* =====
async function rfCN(){ try{
  const url="https://cn.investing.com/rates-bonds/china-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\\d{1,2}\\.\\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const near=t.match(/(\\d{1,2}\\.\\d{1,4})\\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:'=HYPERLINK("https://cn.investing.com/rates-bonds/china-10-year-bond-yield","CN 10Y")' };
  }}catch{} return { v:RF_CN, tag:"兜底", link:"—" }; }
async function rfUS(){ try{
  const url="https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\\d{1,2}\\.\\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const near=t.match(/(\\d{1,2}\\.\\d{1,4})\\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:'=HYPERLINK("https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield","US 10Y")' };
  }}catch{} return { v:RF_US, tag:"兜底", link:"—" }; }
async function rfJP(){ try{
  const url="https://cn.investing.com/rates-bonds/japan-10-year-bond-yield";
  const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:12000 });
  if(r.ok){
    const h=await r.text(); let v=null;
    const m=h.match(/instrument-price-last[^>]*>(\\d{1,2}\\.\\d{1,4})</i); if(m) v=Number(m[1])/100;
    if(!Number.isFinite(v)){ const t=strip(h); const near=t.match(/(\\d{1,2}\\.\\d{1,4})\\s*%/); if(near) v=Number(near[1])/100; }
    if(Number.isFinite(v)&&v>0&&v<1) return { v, tag:"真实", link:'=HYPERLINK("https://cn.investing.com/rates-bonds/japan-10-year-bond-yield","JP 10Y")' };
  }}catch{} return { v:RF_JP, tag:"兜底", link:"—" }; }

async function erpFrom(url, re){ try{
  const r=await fetch(url,{ headers:{ "User-Agent":UA }, timeout:15000 });
  if(r.ok){
    const h=await r.text();
    const row=h.split(/<\\/tr>/i).find(tr=> re.test(tr))||"";
    const nums=[...row.replace(/<[^>]+>/g," ").matchAll(/(\\d{1,2}\\.\\d{1,2})\\s*%/g)].map(m=>Number(m[1]));
    const v=nums.find(x=>x>2&&x<10); if(v!=null) return v/100;
  }}catch{}
  return null;
}
async function erpCN(){ const v=await erpFrom("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html",/China/i); return { v: v??0.0527, tag:v!=null?"真实":"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran China")' }; }
async function erpUS(){ const v=await erpFrom("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html",/(United\\s*States|USA)/i); return { v: v??0.0433, tag:v!=null?"真实":"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran US")' }; }
async function erpJP(){ const v=await erpFrom("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html",/Japan/i); return { v: v??0.0527, tag:v!=null?"真实":"兜底", link:'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran JP")' }; }

// ===== Nikkei：PER =====
async function peNikkei(){
  const url="https://indexes.nikkei.co.jp/en/nkave/archives/data?list=per";
  if (USE_PW) {
    try{
      const { chromium }=await import("playwright");
      const br=await chromium.launch({ headless:true,args:['--disable-blink-features=AutomationControlled'] });
      const ctx=await br.newContext({ userAgent:UA, locale:'en-US', timezoneId:TZ });
      const pg=await ctx.newPage();
      await pg.goto(url,{ waitUntil:'domcontentloaded' });
      await pg.waitForTimeout(1500);
      const v=await pg.evaluate(()=>{
        const tbl=document.querySelector("table"); if(!tbl) return null;
        const rows=tbl.querySelectorAll("tbody tr"); const row=rows[rows.length-1]; if(!row) return null;
        const tds=row.querySelectorAll("td"); if(tds.length<3) return null;
        const txt=(tds[2].textContent||"").trim().replace(/,/g,""); const n=parseFloat(txt);
        return Number.isFinite(n)?n:null;
      });
      await br.close();
      if(Number.isFinite(v)&&v>0&&v<1000) return { v, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
    }catch(e){ dbg("peNikkei PW err",e.message); }
  }
  try{
    const r=await fetch(url,{ headers:{ "User-Agent":UA, "Referer":"https://www.google.com" }, timeout:15000 });
    if(r.ok){
      const h=await r.text(); const trs=[...h.matchAll(/<tr[^>]*>([\\s\\S]*?)<\\/tr>/gi)].map(m=>m[1]);
      let last=null; for(const tr of trs){
        const tds=[...tr.matchAll(/<td[^>]*>([\\s\\S]*?)<\\/td>/gi)].map(m=>m[1].replace(/<[^>]+>/g,"").trim());
        if(tds.length>=3 && /[A-Za-z]{3}\\/\\d{2}\\/\\d{4}/.test(tds[0])){ const n=parseFloat(tds[2].replace(/,/g,"")); if(Number.isFinite(n)) last=n; }
      }
      if(Number.isFinite(last)&&last>0&&last<1000) return { v:last, tag:"真实", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
    }
  }catch(e){ dbg("peNikkei HTTP err",e.message); }
  return { v:"", tag:"兜底", link:`=HYPERLINK("${url}","Nikkei PER (Index Weight Basis)")` };
}

// ===== 写块 & 判定 =====
async function writeBlock(startRow,label,peRes,rfRes,erpStar,erpTag,erpLink,roeRes){
  const { sheetTitle, sheetId }=await ensureToday();
  const pe=(peRes?.v===""||peRes?.v==null)?null:Number(peRes?.v);
  const rf=Number.isFinite(rfRes?.v)?rfRes.v:null;

  let target=erpStar;
  if(label==="沪深300"||label==="中概互联网"||label==="恒生科技") target=ERP_TARGET_CN;

  const roe=Number.isFinite(roeRes?.v)?roeRes.v:null;
  const ep=Number.isFinite(pe)?1/pe:null;
  const factor=(roe!=null&&roe>0)?(roe/ROE_BASE):1;
  const factorDisp=(roe!=null&&roe>0)?Number(factor.toFixed(2)):"";

  const peBuy =(rf!=null&&target!=null)?Number((1/(rf+target+DELTA)*factor).toFixed(2)):null;
  const peSell=(rf!=null&&target!=null&&(rf+target-DELTA)>0)?Number((1/(rf+target-DELTA)*factor).toFixed(2)):null;
  const fairRange=(peBuy!=null&&peSell!=null)?`${peBuy} ~ ${peSell}`:"";

  let status="需手动更新";
  if(Number.isFinite(pe)&&peBuy!=null&&peSell!=null){
    if(pe<=peBuy) status="🟢 买点（低估）";
    else if(pe>=peSell) status="🔴 卖点（高估）";
    else status="🟡 持有（合理）";
  }

  const rows=[
    ["指数",label,"真实","宽基/行业指数估值分块",peRes?.link||"—"],
    ["P/E（TTM）",Number.isFinite(pe)?pe:"",peRes?.tag||(Number.isFinite(pe)?"真实":"兜底"),"估值来源",peRes?.link||"—"],
    ["E/P = 1 / P/E",ep??"",Number.isFinite(pe)?"真实":"兜底","盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）",rf??"",rf!=null?"真实":"兜底",
      (label==="沪深300"||label==="中概互联网"||label==="恒生科技"?"CN 10Y":"US/JP 10Y"),rfRes?.link||"—"],
    ["目标 ERP*",Number.isFinite(target)?target:"",Number.isFinite(target)?"真实":"兜底","达摩达兰",
      erpLink||'=HYPERLINK("https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html","Damodaran")'],
    ["容忍带 δ",DELTA,"真实","减少频繁切换（说明用，不定义卖点）","—"],
    ["买点PE上限（含ROE因子）",peBuy??"",peBuy!=null?"真实":"兜底","买点=1/(r_f+ERP*+δ)×factor","—"],
    ["卖点PE下限（含ROE因子）",peSell??"",peSell!=null?"真实":"兜底","卖点=1/(r_f+ERP*−δ)×factor","—"],
    ["合理PE区间（含ROE因子）",fairRange,(peBuy!=null&&peSell!=null)?"真实":"兜底","买点上限 ~ 卖点下限","—"],
    ["ROE（TTM）",roe??"",roe!=null?"真实":"兜底","盈利能力（小数，显示为百分比）",roeRes?.link||"—"],
    ["ROE基准（可配 env.ROE_BASE）",ROE_BASE,"真实","默认 0.12 = 12%","—"],
    ["ROE倍数因子 = ROE/ROE基准",factorDisp,(factorDisp!=="")?"真实":"兜底","例如 16.4%/12% = 1.36","—"],
    ["说明（公式）","见右","真实","买点=1/(r_f+ERP*+δ)×factor；卖点=1/(r_f+ERP*−δ)×factor；合理区间=买点~卖点","—"],
    ["判定",status,(Number.isFinite(pe)&&peBuy!=null&&peSell!=null)?"真实":"兜底","基于 P/E 与区间","—"],
  ];
  const end=startRow+rows.length-1;
  await write(`'${sheetTitle}'!A${startRow}:E${end}`,rows);

  const requests=[];
  [2,3,4,5,10,11].forEach(i=>{ const r=(startRow-1)+i;
    requests.push({ repeatCell:{ range:{ sheetId, startRowIndex:r,endRowIndex:r+1,startColumnIndex:1,endColumnIndex:2 },
      cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00%" } } }, fields:"userEnteredFormat.numberFormat" }}); });
  [1,6,7,12].forEach(i=>{ const r=(startRow-1)+i;
    requests.push({ repeatCell:{ range:{ sheetId, startRowIndex:r,endRowIndex:r+1,startColumnIndex:1,endColumnIndex:2 },
      cell:{ userEnteredFormat:{ numberFormat:{ type:"NUMBER", pattern:"0.00" } } }, fields:"userEnteredFormat.numberFormat" }}); });
  requests.push({ repeatCell:{ range:{ sheetId, startRowIndex:(startRow-1)+0,endRowIndex:(startRow-1)+1,startColumnIndex:0,endColumnIndex:5 },
    cell:{ userEnteredFormat:{ backgroundColor:{ red:0.95, green:0.95, blue:0.95 }, textFormat:{ bold:true } } }, fields:"userEnteredFormat(backgroundColor,textFormat)" }});
  requests.push({ updateBorders:{ range:{ sheetId, startRowIndex:(startRow-1),endRowIndex:end,startColumnIndex:0,endColumnIndex:5 },
    top:{style:"SOLID",width:1,color:{red:0.8,green:0.8,blue:0.8}},bottom:{style:"SOLID",width:1,color:{red:0.8,green:0.8,blue:0.8}},
    left:{style:"SOLID",width:1,color:{red:0.8,green:0.8,blue:0.8}},right:{style:"SOLID",width:1,color:{red:0.8,green:0.8,blue:0.8}} }});
  await sheets.spreadsheets.batchUpdate({ spreadsheetId:SPREADSHEET_ID, requestBody:{ requests } });

  return { nextRow:end+2, judgment:status, pe };
}

// ===== 邮件 =====
async function sendEmailIfEnabled(lines){
  const { SMTP_HOST,SMTP_PORT,SMTP_USER,SMTP_PASS,MAIL_TO,MAIL_FROM_NAME,MAIL_FROM_EMAIL,FORCE_EMAIL }=process.env;
  if(!SMTP_HOST||!SMTP_PORT||!SMTP_USER||!SMTP_PASS||!MAIL_TO){ dbg("[MAIL] skip env"); return; }
  const transporter=nodemailer.createTransport({ host:SMTP_HOST, port:Number(SMTP_PORT), secure:Number(SMTP_PORT)===465, auth:{ user:SMTP_USER, pass:SMTP_PASS }});
  try{ dbg("[MAIL] verify start",{host:SMTP_HOST,user:SMTP_USER,to:MAIL_TO}); await transporter.verify(); dbg("[MAIL] verify ok"); }
  catch(e){ console.error("[MAIL] verify fail:",e); if(!FORCE_EMAIL) return; console.error("[MAIL] FORCE_EMAIL=1, continue"); }

  const fromEmail=MAIL_FROM_EMAIL||SMTP_USER;
  const from=MAIL_FROM_NAME?`${MAIL_FROM_NAME} <${fromEmail}>`:fromEmail;
  const subject=`Valuation Daily — ${todayStr()} (${TZ})`;
  const text=[`Valuation Daily — ${todayStr()} (${TZ})`,...lines.map(s=>`• ${s}`),"",`See sheet "${todayStr()}" for thresholds & judgments.`].join('\n');
  const html=[`<h3>Valuation Daily — ${todayStr()} (${TZ})</h3>`,`<ul>${lines.map(s=>`<li>${s}</li>`).join("")}</ul>`,`<p>See sheet "${todayStr()}" for thresholds & judgments.</p>`].join("");
  dbg("[MAIL] send start",{subject,to:MAIL_TO,from});
  try{ const info=await transporter.sendMail({ from,to:MAIL_TO,subject,text,html }); console.log("[MAIL] sent",{messageId:info.messageId,response:info.response}); }
  catch(e){ console.error("[MAIL] send error:",e); }
}

// ===== Main =====
(async()=>{
  console.log("[INFO] Run start",todayStr(),"USE_PLAYWRIGHT=",USE_PW,"TZ=",TZ);
  let row=1;
  const { sheetTitle, sheetId }=await ensureToday();
  await clearTodaySheet(sheetTitle,sheetId);

  // VC：从脚本/NUXT 数据解析（Playwright）
  let vcMap={};
  if(USE_PW){
    try{ vcMap=await fetchVCMapPW(); }catch(e){ dbg("VCMap err",e.message); vcMap={}; }
  }

  // HS300（VC；CN10Y；ERP* China）
  const rec_hs=vcMap["SH000300"];
  const pe_hs=rec_hs?.pe?{v:rec_hs.pe,tag:"真实",link:`=HYPERLINK("${VC_URL}","VC SH000300")`}:{v:PE_OVERRIDE_CN??"",tag:"兜底",link:"—"};
  const rf_cn=await rfCN(); const roe_hs=rec_hs?.roe?{v:rec_hs.roe,tag:"真实",link:`=HYPERLINK("${VC_URL}","VC")`}:{v:"",tag:"兜底",link:"—"};
  let r=await writeBlock(row,"沪深300",pe_hs,rf_cn,ERP_TARGET_CN,"真实",null,roe_hs);
  row=r.nextRow; const j_hs=r.judgment; const pv_hs=r.pe;

  // SP500（VC；US10Y；ERP* US）
  const rec_sp=vcMap["SP500"];
  const pe_sp=rec_sp?.pe?{v:rec_sp.pe,tag:"真实",link:`=HYPERLINK("${VC_URL}","VC SP500")`}:{v:PE_OVERRIDE_SPX??"",tag:"兜底",link:"—"};
  const rf_us=await rfUS(); const { v:erp_us_v,tag:erp_us_tag,link:erp_us_link }=await erpUS();
  const roe_sp=rec_sp?.roe?{v:rec_sp.roe,tag:"真实",link:`=HYPERLINK("${VC_URL}","VC")`}:{v:"",tag:"兜底",link:"—"};
  r=await writeBlock(row,"标普500",pe_sp,rf_us,erp_us_v,erp_us_tag,erp_us_link,roe_sp);
  row=r.nextRow; const j_sp=r.judgment; const pv_sp=r.pe;

  // Nikkei（官方 PER；ROE 可覆写）
  const pe_nk=await peNikkei(); const rf_jp=await rfJP(); const { v:erp_jp_v,tag:erp_jp_tag,link:erp_jp_link }=await erpJP();
  const roe_nk=(ROE_JP!=null)?{v:ROE_JP,tag:"覆写",link:"—"}:{v:null,tag:"兜底",link:"—"};
  r=await writeBlock(row,"日经指数",pe_nk,rf_jp,erp_jp_v,erp_jp_tag,erp_jp_link,roe_nk);
  row=r.nextRow; const j_nk=r.judgment; const pv_nk=r.pe;

  // 中概互联网（VC；CN10Y；ERP* China）
  const rec_cx=vcMap["CSIH30533"];
  const pe_cx=rec_cx?.pe?{v:rec_cx.pe,tag:"真实",link:`=HYPERLINK("${VC_URL}","VC CSIH30533")`}:{v:PE_OVERRIDE_CXIN??"",tag:"兜底",link:"—"};
  const rf_cn2=await rfCN(); const { v:erp_cn_v,tag:erp_cn_tag,link:erp_cn_link }=await erpCN();
  const roe_cx=rec_cx?.roe?{v:rec_cx.roe,tag:"真实",link:`=HYPERLINK("${VC_URL}","VC")`}:{v:"",tag:"兜底",link:"—"};
  r=await writeBlock(row,"中概互联网",pe_cx,rf_cn2,erp_cn_v,erp_cn_tag,erp_cn_link,roe_cx);
  row=r.nextRow; const j_cx=r.judgment; const pv_cx=r.pe;

  // 恒生科技（VC；与中概同口径：CN10Y；ERP* China）
  const rec_hst=vcMap["HSTECH"];
  const pe_hst=rec_hst?.pe?{v:rec_hst.pe,tag:"真实",link:`=HYPERLINK("${VC_URL}","VC HSTECH")`}:{v:PE_OVERRIDE_HSTECH??"",tag:"兜底",link:"—"};
  const rf_cn3=await rfCN(); const { v:erp_hk_v,tag:erp_hk_tag,link:erp_hk_link }=await erpCN();
  const roe_hst=rec_hst?.roe?{v:rec_hst.roe,tag:"真实",link:`=HYPERLINK("${VC_URL}","VC")`}:{v:"",tag:"兜底",link:"—"};
  r=await writeBlock(row,"恒生科技",pe_hst,rf_cn3,erp_hk_v,erp_hk_tag,erp_hk_link,roe_hst);
  row=r.nextRow; const j_hst=r.judgment; const pv_hst=r.pe;

  console.log("[DONE]",todayStr(),{ hs300_pe:pe_hs?.v, spx_pe:pe_sp?.v, nikkei_pe:pe_nk?.v, cxin_pe:pe_cx?.v, hstech_pe:pe_hst?.v });

  const lines=[
    `HS300 PE: ${pv_hs??"-"} → ${j_hs??"-"}`,
    `SPX PE: ${pv_sp??"-"} → ${j_sp??"-"}`,
    `Nikkei PE: ${pv_nk??"-"} → ${j_nk??"-"}`,
    `China Internet PE: ${pv_cx??"-"} → ${j_cx??"-"}`,
    `HSTECH PE: ${pv_hst??"-"} → ${j_hst??"-"}`
  ];
  await sendEmailIfEnabled(lines);
})();
