name: Daily Valuation (10:00 Beijing)

on:
  schedule:
    - cron: '0 2 * * *'   # 02:00 UTC = 北京时间 10:00
  workflow_dispatch: {}

jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 10

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'

      - name: Install deps
        run: npm ci || npm i

      # Playwright 浏览器安装（仅在 USE_PLAYWRIGHT=1 时执行）
      - name: Install Playwright Chromium (optional)
        if: env.USE_PLAYWRIGHT == '1'
        run: npx playwright install chromium --with-deps

      - name: Run fetch & write
        env:
          # Google 服务账号
          GOOGLE_CLIENT_EMAIL: ${{ secrets.GOOGLE_CLIENT_EMAIL }}
          GOOGLE_PRIVATE_KEY:  ${{ secrets.GOOGLE_PRIVATE_KEY }}
          SPREADSHEET_ID:      ${{ vars.SPREADSHEET_ID }}

          # 邮件（可选）
          SMTP_HOST: ${{ secrets.SMTP_HOST }}
          SMTP_PORT: ${{ secrets.SMTP_PORT }}
          SMTP_USER: ${{ secrets.SMTP_USER }}
          SMTP_PASS: ${{ secrets.SMTP_PASS }}
          MAIL_TO:   ${{ vars.MAIL_TO }}
          MAIL_FROM_NAME: ${{ vars.MAIL_FROM_NAME }}

          # 判定参数（默认 5.27% / 0.50%）
          ERP_TARGET: ${{ vars.ERP_TARGET }}
          DELTA:      ${{ vars.DELTA }}

          # Playwright 开关
          USE_PLAYWRIGHT: '1'

          # 兜底值（只在真实抓取失败时用）
          RF_OVERRIDE: '0.0178'
          PE_OVERRIDE: ''

          TZ: Asia/Shanghai
        run: node scripts/fetch_pe_and_write_sheet.mjs
