// === Danjuan PE → Google Sheet (Existing Workbook, daily new tab + email) ===
// 基线：不启用 Playwright；P/E=蛋卷(JSON→HTML)；10Y=有知有行(文本)；
// 兜底：PE_OVERRIDE（变量，可空）、RF_OVERRIDE=1.78%（默认）
// 每日新建 YYYY-MM-DD 标签页；SMTP 未配置自动跳过发信；数据源标注“真实/兜底”。

import fetch from "node-fetch";
import nodemailer from "nodemailer";
import { google } from "googleapis";

// ---------- 小工具 ----------
const numOrDefault = (v, d) => {
  if (v === undefined || v === null) return d;
  const s = String(v).trim();
  if (s === "") return d;
  const n = Number(s);
  return Number.isFinite(n) ? n : d;
};
const todayStr = (tz = "Asia/Shanghai") => {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: tz }));
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
};

// ---------- 常量 / 环境 ----------
const TZ = process.env.TZ || "Asia/Shanghai";
const ERP_TARGET = numOrDefault(process.env.ERP_TARGET, 0.0527);
const DELTA = numOrDefault(process.env.DELTA, 0.005);
const RF_OVERRIDE = numOrDefault(process.env.RF_OVERRIDE, 0.0178);
const PE_OVERRIDE = (() => {
  const s = (process.env.PE_OVERRIDE ?? "").toString().trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) && n > 0 && n < 1000 ? n : null;
})();

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
if (!SPREADSHEET_ID) {
  console.error("缺少 SPREADSHEET_ID 环境变量");
  process.exit(1);
}

const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL,
  null,
  (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
  ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version: "v4", auth });

// ---------- 数据抓取 ----------
async function getPE() {
  // 优先：蛋卷 JSON（参数式 & REST 式都试一次）
  let pe = null;
  let source = "兜底";

  try {
    const r1 = await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail?index_code=SH000300", {
      headers: { "User-Agent": "Mozilla/5.0", Referer: "https://danjuanfunds.com" },
      timeout: 8000
    });
    if (r1.ok) {
      const j = await r1.json();
      const v = Number(j?.data?.pe_ttm ?? j?.data?.pe ?? j?.data?.valuation?.pe_ttm);
      if (Number.isFinite(v) && v > 0 && v < 1000) {
        pe = v;
        source = "真实";
      }
    }
  } catch {}

  if (pe == null) {
    try {
      const r2 = await fetch("https://danjuanfunds.com/index-detail/SH000300", {
        headers: { "User-Agent": "Mozilla/5.0" },
        timeout: 8000
      });
      if (r2.ok) {
        const html = await r2.text();
        // 内嵌 JSON："pe_ttm": 13.97 或 "pe_ttm":"13.97"
        const m = html.match(/"pe_ttm"\s*:\s*"?([0-9]+(?:\.[0-9]+)?)"?/i);
        if (m) {
          const v = Number(m[1]);
          if (Number.isFinite(v) && v > 0 && v < 1000) {
            pe = v;
            source = "真实";
          }
        } else {
          // 文本回退
          const text = html.replace(/<[^>]+>/g, " ");
          const regs = [
            /PE[^0-9]{0,6}([0-9]+(?:\.[0-9]+)?)/i,
            /市盈率（?TTM）?[^0-9]{0,10}([0-9]+(?:\.[0-9]+)?)/,
          ];
          for (const re of regs) {
            const mm = text.match(re);
            if (mm) {
              const v = Number(mm[1]);
              if (Number.isFinite(v) && v > 0 && v < 1000) {
                pe = v;
                source = "真实";
                break;
              }
            }
          }
        }
      }
    } catch {}
  }

  if (pe == null && PE_OVERRIDE != null) {
    pe = PE_OVERRIDE;
    source = "兜底";
  }

  return { pe, source };
}

async function getRF() {
  // 有知有行（文本）
  let rf = null;
  let source = "兜底";
  try {
    const res = await fetch("https://youzhiyouxing.cn/data", {
      headers: { "User-Agent": "Mozilla/5.0" },
      timeout: 6000
    });
    if (res.ok) {
      const html = await res.text();
      const m = html.match(/10年期国债到期收益率[^%]{0,120}?(\d+(?:\.\d+)?)\s*%/);
      if (m) {
        const v = Number(m[1]) / 100;
        if (Number.isFinite(v) && v > 0 && v < 1) {
          rf = v; source = "真实";
        }
      }
    }
  } catch {}
  if (rf == null) { rf = RF_OVERRIDE; source = "兜底"; }
  return { rf, source };
}

// ---------- 表操作：新建或获取“当日标签”，写入并格式化 ----------
async function upsertDailySheet(values, tz = TZ) {
  const title = todayStr(tz);
  // 取元信息，找目标 sheetId；没有就创建
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  let sheet = meta.data.sheets?.find(s => s.properties?.title === title);
  if (!sheet) {
    const addRes = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title } } }] }
    });
    const added = addRes.data?.replies?.[0]?.addSheet?.properties;
    sheet = { properties: added };
  }
  const range = `'${title}'!A1:D${values.length}`;

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range,
    valueInputOption: "USER_ENTERED",
    requestBody: { values }
  });

  // 设置数值格式（B列）
  const sheetId = sheet.properties.sheetId;
  const cell = (r0) => ({
    sheetId, startRowIndex: r0, endRowIndex: r0 + 1, startColumnIndex: 1, endColumnIndex: 2
  });

  const reqs = [];
  // 第4~8行（0基：3~7）显示百分比：E/P、r_f、隐含ERP、ERP*、δ
  [3,4,5,6,7].forEach(r => reqs.push({
    repeatCell: { range: cell(r), cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00%" } } }, fields: "userEnteredFormat.numberFormat" }
  }));
  // 第3行 & 第9行（0基：2、8）两位小数：P/E、P/E上限
  [2,8].forEach(r => reqs.push({
    repeatCell: { range: cell(r), cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern: "0.00" } } }, fields: "userEnteredFormat.numberFormat" }
  }));

  if (reqs.length) {
    await sheets.spreadsheets.batchUpdate({ spreadsheetId: SPREADSHEET_ID, requestBody: { requests: reqs } });
  }
}

// ---------- 邮件（未配 SMTP 自动跳过） ----------
async function maybeSendEmail(payload) {
  const { SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_TO, MAIL_FROM_NAME } = process.env;
  if (!SMTP_HOST || !SMTP_USER || !SMTP_PASS || !MAIL_TO) {
    console.warn("[MAIL] 未配置 SMTP，跳过邮件发送。");
    return;
  }
  const transporter = nodemailer.createTransport({
    host: SMTP_HOST,
    port: Number(SMTP_PORT || 465),
    secure: Number(SMTP_PORT || 465) === 465,
    auth: { user: SMTP_USER, pass: SMTP_PASS }
  });

  const pct = v => v == null ? "-" : (v * 100).toFixed(2) + "%";
  const html = `
  <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.6">
    <p>已更新：<b>${payload.date}</b></p>
    <ul>
      <li>P/E（TTM）：<b>${payload.pe ?? "-"}</b>（${payload.peSource}）</li>
      <li>E/P：<b>${pct(payload.ep)}</b></li>
      <li>10Y名义：<b>${pct(payload.rf)}</b>（${payload.rfSource}）</li>
      <li>隐含ERP：<b>${pct(payload.impliedERP)}</b></li>
      <li>P/E 上限：<b>${payload.peLimit ?? "-"}</b></li>
      <li>判定：<b>${payload.status}</b></li>
    </ul>
    <p><a href="https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/edit#gid=0" target="_blank">在线打开「总表」</a></p>
  </div>`;

  await transporter.sendMail({
    from: `"${MAIL_FROM_NAME || "Valuation Bot"}" <${SMTP_USER}>`,
    to: MAIL_TO,
    subject: `[估值] 沪深300（${payload.date}）— ${payload.status}`,
    html
  });
  console.log("[MAIL] sent to", MAIL_TO);
}

// ---------- 主流程 ----------
(async () => {
  const date = todayStr(TZ);

  // 1) 抓数据
  const peRes = await getPE();         // {pe, source: '真实/兜底'}
  const rfRes = await getRF();         // {rf, source: '真实/兜底'}

  // 2) 计算（仅在有数据时计算）
  const ep = peRes.pe ? (1 / peRes.pe) : null;
  const impliedERP = (ep != null && rfRes.rf != null) ? (ep - rfRes.rf) : null;
  const peLimit = (rfRes.rf != null) ? Number((1 / (rfRes.rf + ERP_TARGET)).toFixed(2)) : null;

  let status = "需手动更新";
  if (impliedERP != null) {
    if (impliedERP >= ERP_TARGET + DELTA) status = "买点（低估）";
    else if (impliedERP <= ERP_TARGET - DELTA) status = "卖点（高估）";
    else status = "持有（合理）";
  }

  // 3) 组织写入值（注意：展示为百分比的列我们仍填“小数”，格式化时再设 %）
  const values = [
    ["字段","数值","说明","数据源"],
    ["指数","沪深300","本工具演示以沪深300为例，可扩展","=HYPERLINK(\"https://www.csindex.com.cn/zh-CN/indices/index-detail/000300\",\"中证指数有限公司\")"],
    ["P/E（TTM）", peRes.pe, "蛋卷基金 index-detail（JSON→HTML）","=HYPERLINK(\"https://danjuanfunds.com/index-detail/SH000300\",\"Danjian - " + peRes.source + "\")"],
    ["E/P = 1 / P/E", ep, "盈收益率（小数，显示为百分比）","—"],
    ["无风险利率 r_f（10Y名义）", rfRes.rf, "有知有行（文本；抓不到用兜底）","=HYPERLINK(\"https://youzhiyouxing.cn/data\",\"Youzhiyouxing - " + rfRes.source + "\")"],
    ["隐含ERP = E/P − r_f", impliedERP, "市场给予的风险补偿（小数，显示为百分比）","—"],
    ["目标 ERP*", ERP_TARGET, "建议参考达摩达兰","=HYPERLINK(\"https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html\",\"Damodaran\")"],
    ["容忍带 δ", DELTA, "减少频繁切换","—"],
    ["对应P/E上限 = 1/(r_f + ERP*)", peLimit, "直观对照","—"],
    ["判定", status, "买点/持有/卖点/需手动","—"],
    ["信号图标", (status.startsWith("买点")?"🟢":status.startsWith("卖点")?"🔴":status==="持有（合理）"?"🟡":"⚪"), "🟢=买点，🟡=持有，🔴=卖点，⚪=需手动","—"]
  ];

  await upsertDailySheet(values, TZ);

  console.log("[DEBUG]", { date, pe: peRes.pe, peSource: peRes.source, rf: rfRes.rf, rfSource: rfRes.source, ep, impliedERP, peLimit, status });

  // 4) 邮件（可选）
  await maybeSendEmail({ date, pe: peRes.pe, peSource: peRes.source, rf: rfRes.rf, rfSource: rfRes.source, ep, impliedERP, peLimit, status });
})().catch(e => { console.error(e); process.exit(1); });
