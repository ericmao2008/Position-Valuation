# Danjuan PE → Google Sheet 自动化（Playwright + Google Sheets）

目标：每天 **09:30（北京时间）** 抓取 **蛋卷基金 index-detail/SH000300** 的沪深300 **P/E（TTM）**，
并创建名为 **“持仓估值YYYY-MM-DD”** 的 Google 表格，输出 ERP 判定与红黄绿信号。

## 你将得到什么
- `npm run login`：打开真实浏览器（非无头），你 **自己** 在页面里完成“手机+验证码”登录；程序会把登录后的 **cookies/localStorage** 保存到 `storage_state.json`。
- `npm run fetch`：在无头浏览器里重用 `storage_state.json`，访问 `index-detail/SH000300`，抓取 **PE（TTM）**；同时抓取 **10Y 国债收益率**；再把结果写入一份新的 Google 表格，并根据 ERP 规则给出 **🟢/🟡/🔴**。

> 你的手机号和验证码 **不会**经过脚本传输给任何人，也不需要告诉我；你只是在你自己的浏览器里登录一次，脚本只保存登录后的会话状态。
> 如果后续会话过期，重新 `npm run login` 一次即可。

## 本地快速开始
1. 安装 Node 20+ ： https://nodejs.org/
2. 克隆项目并安装依赖：
   ```bash
   npm i
   npm run playwright-install
   ```
3. 准备 Google 服务账号（用于创建表格）：
   - 在 GCP 创建 Service Account，授予 Drive & Sheets 权限；生成密钥。
   - 在 `.env`（或 CI 的 Secrets）里设置：
     - `GOOGLE_CLIENT_EMAIL`
     - `GOOGLE_PRIVATE_KEY`（注意换行写成 `\n` 形式）
     - （可选）`GOOGLE_FOLDER_ID`：把新表保存到此文件夹。
4. 一次性登录蛋卷：
   ```bash
   npm run login
   # 弹出浏览器 → 进入 https://danjuanfunds.com/index-detail/SH000300 → 在页面里完成“手机+验证码”登录
   # 登录成功后等待几秒，程序会保存 storage_state.json
   ```
5. 运行抓取与写表：
   ```bash
   npm run fetch
   ```

## GitHub Actions 定时运行（09:30 北京时间）
- 仓库 → Settings → Secrets and variables：
  - **Secrets**：设置 `GOOGLE_CLIENT_EMAIL`、`GOOGLE_PRIVATE_KEY`、（可选）`GOOGLE_FOLDER_ID`。
  - **Variables**（可选）：`ERP_TARGET`、`DELTA`。
- 首次登录如何在 CI 生效？
  - 在本地 `npm run login` 得到 `storage_state.json`，在仓库 Actions 中手动运行一次 `workflow_dispatch`，它会把本地的 `storage_state.json` 上传为 Artifact（或直接把文件提交到仓库私有分支）。
  - 之后 Actions 每天运行会下载该 Artifact 并复用登录状态。若过期，再本地登陆一次并触发上传。

## 字段口径
- **PE（TTM）**：来自 `index-detail/SH000300` 页面加载的 `djapi/index_evaluation/detail` JSON 或页面文本。
- **10Y 国债收益率**：东方财富“中美国债收益率”页（回退 Investing）；解析失败则表格显示“需手动更新”。
- **ERP 判定**：隐含ERP = 1/PE − r_f；买点/持有/卖点采用你在环境变量中的 `ERP_TARGET` 和 `DELTA`。

## 常见问题
- **一定要把手机号/验证码给脚本吗？** 不需要。你只需在本机弹出的真实浏览器里登录一次，脚本保存会话即可。
- **会话多久过期？** 由目标站点策略决定（常见 7~30 天）。过期时再 `npm run login` 一次。
- **能用 Google Apps Script 完成登录吗？** 不能。Apps Script 不带浏览器与 JS 引擎，无法处理此类需要 JS 与登录保护的页面。
