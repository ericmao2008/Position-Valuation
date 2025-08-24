import fetch from "node-fetch";
import { google } from "googleapis";
import nodemailer from "nodemailer";

// Google Sheets API Auth
const auth = new google.auth.JWT(
  process.env.GOOGLE_CLIENT_EMAIL,
  null,
  (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
  ["https://www.googleapis.com/auth/spreadsheets"]
);

const sheets = google.sheets({ version: "v4", auth });

// 获取 P/E 数据
async function getPE() {
  let source = "兜底";
  let pe = process.env.PE_OVERRIDE ? parseFloat(process.env.PE_OVERRIDE) : null;

  try {
    const res = await fetch("https://danjuanfunds.com/djapi/index_evaluation/detail/SH000300");
    if (res.ok) {
      const data = await res.json();
      if (data?.data?.pe_ttm) {
        pe = parseFloat(data.data.pe_ttm);
        source = "真实";
      }
    }
  } catch (err) {
    console.warn("抓取 PE 失败，使用兜底值");
  }

  return { pe, source };
}

// 获取无风险利率
async function getRF() {
  let source = "兜底";
  let rf = process.env.RF_OVERRIDE ? parseFloat(process.env.RF_OVERRIDE) : null;

  try {
    const res = await fetch("https://youzhiyouxing.cn/data");
    const text = await res.text();
    const match = text.match(/10年期国债到期收益率[^0-9]*([\d.]+)%/);
    if (match) {
      rf = parseFloat(match[1]) / 100.0;
      source = "真实";
    }
  } catch (err) {
    console.warn("抓取 RF 失败，使用兜底值");
  }

  return { rf, source };
}

// 写入 Google Sheet
async function writeSheet(peResult, rfResult) {
  const values = [
    ["字段", "数值", "说明", "数据源"],
    ["指数", "沪深300", "本工具演示以沪深300为例", "中证指数有限公司"],
    ["P/E (TTM)", peResult.pe, "蛋卷基金 HTML/JSON", "Danjian - " + peResult.source],
    ["E/P = 1/PE", (1 / peResult.pe).toFixed(4), "盈收益率（小数，显示为百分比）", "—"],
    ["无风险利率 r_f", (rfResult.rf * 100).toFixed(2) + "%", "有知有行（仅用该站）", "Youzhiyouxing - " + rfResult.source],
    ["目标 ERP*", process.env.ERP_TARGET || "5.27%", "参考达摩达兰", "Damodaran"],
    ["容忍带 δ", process.env.DELTA || "0.50%", "减少频繁切换", "—"]
  ];

  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.SPREADSHEET_ID,
    range: "Sheet1!A1:D7",
    valueInputOption: "RAW",
    requestBody: { values }
  });
}

// 邮件推送
async function sendMail(peResult, rfResult) {
  if (!process.env.SMTP_HOST) {
    console.log("未配置 SMTP，跳过邮件发送");
    return;
  }

  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: process.env.SMTP_PORT,
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });

  const mail = {
    from: `"${process.env.MAIL_FROM_NAME}" <${process.env.SMTP_USER}>`,
    to: process.env.MAIL_TO,
    subject: `沪深300 估值日报`,
    text: `PE: ${peResult.pe} (${peResult.source})\nRF: ${rfResult.rf} (${rfResult.source})`
  };

  await transporter.sendMail(mail);
}

// 主流程
(async () => {
  const peResult = await getPE();
  const rfResult = await getRF();
  await writeSheet(peResult, rfResult);
  await sendMail(peResult, rfResult);
})();
