# 日经指数PE/PB抓取修复说明

## 问题描述
之前的版本中，日经指数的PE和PB值使用Google Sheets的`IMPORTXML`函数获取，这种方式存在以下问题：
- 数据获取不稳定，经常失败
- 依赖Google Sheets的网络连接
- 无法在脚本执行时验证数据有效性

## 修复方案
新增了`fetchNikkei()`函数，使用Playwright直接抓取日经指数数据：

### 1. 多数据源支持
- **主要数据源**: 日经官网 (https://indexes.nikkei.co.jp/en/nkave/)
- **备用数据源**: Investing.com (https://www.investing.com/indices/japan-ni225)

### 2. 抓取逻辑
```javascript
async function fetchNikkei() {
  // 方法1: 从日经官网抓取PE
  // 方法2: 从日经官网抓取PB  
  // 方法3: 从Investing.com抓取备用数据
  // 返回: { pe, pb, link }
}
```

### 3. 错误处理
- 每个数据源独立处理，一个失败不影响其他
- 详细的错误日志记录
- 优雅降级到备用数据源

## 使用方法

### 测试日经指数抓取
```bash
# 方法1: 使用新的测试模式
node scripts/fetch_pe_and_write_sheet.mjs --mode=test-nikkei

# 方法2: 使用专门的测试脚本
node scripts/test_nikkei.mjs
```

### 完整流程测试
```bash
# 测试整个估值流程（包含日经指数）
node scripts/fetch_pe_and_write_sheet.mjs --mode=test-sheet
```

## 数据输出
修复后的日经指数数据将显示为：
- **P/E (TTM)**: 真实抓取值（不再是公式）
- **P/B (TTM)**: 真实抓取值（不再是公式）
- **数据来源**: "真实"（而不是"Formula"）
- **链接**: 指向日经官网

## 兼容性
- 保持与现有代码的完全兼容
- 不影响其他指数的处理逻辑
- 支持DRY模式测试

## 版本信息
- **修复版本**: V6.1.0
- **修复日期**: 2024年
- **主要变更**: 新增fetchNikkei函数，修复日经指数数据抓取问题

