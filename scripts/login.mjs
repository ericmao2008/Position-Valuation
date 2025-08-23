import { chromium } from 'playwright';
import fs from 'fs';

const STORAGE = process.env.STORAGE_STATE || 'storage_state.json';
const URL = 'https://danjuanfunds.com/index-detail/SH000300';

(async () => {
  console.log('> Launching a real browser...');
  const browser = await chromium.launch({ headless: false, slowMo: 50 });
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto(URL, { waitUntil: 'domcontentloaded' });
  console.log('> 请在浏览器里完成 “手机号码 + 验证码” 登录（不需要把验证码告诉程序或任何人）。');
  console.log('> 登录成功后，保持页面停留 3-5 秒，程序会自动保存登录状态。');
  // 观察本地存储/登录区块是否出现，最多等待 2 分钟
  try {
    await page.waitForTimeout(5000);
    // 允许用户手动操作登录流程
    await page.waitForTimeout(60_000);
  } catch (e) {}
  await context.storageState({ path: STORAGE });
  console.log(`> 已保存登录状态到 ${STORAGE}`);
  await browser.close();
})().catch(e => {
  console.error(e);
  process.exit(1);
});
