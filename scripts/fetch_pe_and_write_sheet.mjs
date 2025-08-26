// ===== Tencent: Market Cap & Shares (Playwright DOM first, then Yahoo APIs) =====
async function fetchTencentData() {
  // helpers
  const parseAbbrev = (txt) => {
    if (!txt) return null;
    const s = String(txt).trim().toUpperCase(); // e.g. "2.96T" "950.3B" "120.5M"
    const m = s.match(/([\d.,]+)\s*([KMBT]?)/);
    if (!m) return null;
    const n = parseFloat(m[1].replace(/,/g, ""));
    if (!Number.isFinite(n)) return null;
    const unit = m[2] || "";
    const mul = unit === "T" ? 1e12 : unit === "B" ? 1e9 : unit === "M" ? 1e6 : unit === "K" ? 1e3 : 1;
    return n * mul;
  };
  const toNum = (v) => (v != null && Number.isFinite(Number(v)) ? Number(v) : null);

  // 1) Primary path when USE_PW=true: scrape Yahoo Finance DOM (更稳)
  if (USE_PW) {
    try {
      const { chromium } = await import("playwright");
      const br  = await chromium.launch({ headless: true, args: ['--disable-blink-features=AutomationControlled'] });
      const ctx = await br.newContext({ userAgent: UA, locale: 'zh-CN', timezoneId: TZ });
      const pg  = await ctx.newPage();

      // 1a) 报价页，拿“市值”
      const qUrl = "https://finance.yahoo.com/quote/0700.HK/";
      await pg.goto(qUrl, { waitUntil: "domcontentloaded", timeout: 25000 });
      // 若出现 cookie banner，尝试点击同意
      try {
        const btn = await pg.$('button:has-text("Accept all")');
        if (btn) await btn.click();
      } catch {}
      await pg.waitForLoadState("networkidle").catch(()=>{});
      // 统一等待一下渲染
      await pg.waitForTimeout(1200);

      // 市值在 data-test="MARKET_CAP-value"
      const mcText = await pg.$eval('[data-test="MARKET_CAP-value"]', el => el.textContent).catch(()=>null);
      let marketCap = parseAbbrev(mcText);

      // 1b) 关键统计页，拿“Shares Outstanding”（总股本）
      const ksUrl = "https://finance.yahoo.com/quote/0700.HK/key-statistics";
      await pg.goto(ksUrl, { waitUntil: "domcontentloaded", timeout: 25000 });
      await pg.waitForTimeout(1200);
      // 尝试多个字段名，避免文案变动：SHARES_OUTSTANDING-value 或 Shares Outstanding 行
      let sharesTxt = null;
      try {
        sharesTxt = await pg.$eval('[data-test="SHARES_OUTSTANDING-value"]', el => el.textContent);
      } catch {
        // 退而求其次：扫描表格，把 “Shares Outstanding” 对应的后一格文本拿到
        sharesTxt = await pg.evaluate(()=>{
          const rows = Array.from(document.querySelectorAll('table tr'));
          for (const tr of rows) {
            const tds = Array.from(tr.querySelectorAll('td,th'));
            if (tds.length >= 2) {
              const k = (tds[0].textContent || '').trim();
              if (/Shares\s+Outstanding/i.test(k)) {
                return (tds[1].textContent || '').trim();
              }
            }
          }
          return null;
        });
      }
      let totalShares = parseAbbrev(sharesTxt);

      await br.close();

      // 两个关键字段若都拿到，则返回
      if (marketCap && totalShares) {
        return { marketCap, totalShares };
      }
      // 如果只拿到其一，不立即返回，继续走 JSON API 回退，争取把另一个也补齐
    } catch (e) {
      dbg("Tencent(PW DOM) err", e.message);
    }
  }

  // 2) 回退一：Yahoo Finance quoteSummary API（有时需 crumb，可能偶发失败）
  try {
    const url = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/0700.HK?modules=price,defaultKeyStatistics";
    const r = await fetch(url, {
      headers: {
        "User-Agent": UA,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
      },
      timeout: 15000
    });
    if (r.ok) {
      const j = await r.json();
      const res = j?.quoteSummary?.result?.[0] || {};
      const mc =
        res?.price?.marketCap?.raw ??
        res?.defaultKeyStatistics?.enterpriseValue?.raw ??
        null;
      const shares =
        res?.defaultKeyStatistics?.sharesOutstanding?.raw ??
        res?.price?.sharesOutstanding?.raw ??
        null;

      const marketCap   = toNum(mc);
      const totalShares = toNum(shares);

      if (marketCap && totalShares) return { marketCap, totalShares };
      // 半残数据先记下，最后兜底时返回
      var partial_mc = marketCap || null;
      var partial_sh = totalShares || null;

      // 3) 回退二：Yahoo Finance quote API
      try {
        const url2 = "https://query2.finance.yahoo.com/v7/finance/quote?symbols=0700.HK";
        const r2 = await fetch(url2, {
          headers: { "User-Agent": UA },
          timeout: 12000
        });
        if (r2.ok) {
          const j2 = await r2.json();
          const res2 = j2?.quoteResponse?.result?.[0] || {};
          const marketCap2   = toNum(res2?.marketCap);
          const totalShares2 = toNum(res2?.sharesOutstanding);

          const mcFinal = marketCap   || marketCap2   || null;
          const shFinal = totalShares || totalShares2 || null;

          if (mcFinal && shFinal) return { marketCap: mcFinal, totalShares: shFinal };
          if (mcFinal || shFinal)  return { marketCap: mcFinal || partial_mc, totalShares: shFinal || partial_sh };
        }
      } catch (e2) {
        dbg("Tencent(quote) err", e2.message);
      }

      if (partial_mc || partial_sh) return { marketCap: partial_mc, totalShares: partial_sh };
    }
  } catch (e) {
    dbg("Tencent(YF) err", e.message);
  }

  // 4) 全部失败
  return { marketCap: null, totalShares: null };
}
