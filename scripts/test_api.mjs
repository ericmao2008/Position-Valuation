import fetch from "node-fetch";

// 这是一个简化的日志函数，仅用于测试
const dbg = (...a) => console.log("[DEBUG]", ...a);

// ===== Danjuan API: Index Valuations =====
// 将要被测试的核心函数
async function fetchIndexValuations() {
  const url = "https://danjuanfunds.com/djapi/v3/index/family/list";
  const options = {
    method: 'get',
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    },
    timeout: 20000 // 设置20秒超时
  };
  
  try {
    const response = await fetch(url, options);
    if (!response.ok) {
      console.error(`[ERROR] Danjuan API request failed with status: ${response.status}`);
      return {};
    }
    const json = await response.json();
    const items = json.data;
    const valuationMap = {};
    
    // 将API返回的数组转换为一个方便查找的对象
    for (const item of items) {
      valuationMap[item.name] = {
        pe: item.pe,
        pb: item.pb
      };
    }
    dbg("Danjuan valuations fetched:", valuationMap);
    return valuationMap;
  } catch (e) {
    console.error(`[ERROR] Failed to fetch or parse Danjuan API: ${e.message}`);
    return {}; // 返回空对象以防主流程中断
  }
}

// ===== 测试执行区域 =====
(async () => {
  console.log("▶️  正在开始测试蛋卷基金API...");
  
  const valuations = await fetchIndexValuations();
  
  console.log("\n✅  测试完成，API返回的原始结果如下：");
  console.log(valuations);

  if (Object.keys(valuations).length > 0 && valuations["沪深300"]) {
    console.log("\n🎉  测试成功！");
    console.log("API可以正常访问并返回了有效的数据结构。");
    console.log(`其中，沪深300的PE为: ${valuations["沪深300"].pe}`);
  } else {
    console.log("\n❌  测试失败。");
    console.log("API未能返回有效数据，请检查上面打印的 [ERROR] 日志。");
  }
})();
