# 应用统计研究模块使用说明

## 1. 目标

本模块用于把系统从“偏工程的量化交易平台”扩展为“可直接展示统计学能力的应用统计研究平台”。
即使评审老师不熟悉金融交易工程，也可以按统计研究流程理解项目。

## 2. 统计学方法清单

- 描述统计：均值/中位数/标准差/分位数/偏度/峰度
- 分布检验：Jarque-Bera 正态性检验
- 假设检验：双样本均值检验（Welch t + 置换检验）
- 回归建模：OLS（系数、标准误、显著性、置信区间）
- 模型诊断：VIF、Durbin-Watson、残差正态性
- 区间估计：Bootstrap 95% 置信区间

## 3. API

- `POST /applied-stats/descriptive`
- `POST /applied-stats/tests/two-sample-mean`
- `POST /applied-stats/model/ols`
- `POST /applied-stats/cases/market-factor-study`
- `GET /applied-stats/showcase`（前端展示页面）

其中 `market-factor-study` 会自动执行：

1. 构建研究数据集（收益率、动量、波动率、流动性、基本面得分）
2. 描述统计和相关分析
3. 高动量组 vs 低动量组均值差异检验
4. OLS 回归与诊断
5. 输出可解释的结论文本

可选 `export_markdown=true` 将研究报告导出到 `reports/`。

## 4. CLI

```bash
python -m trading_assistant.cli applied-stats-study --symbol 000001 --start-date 2025-01-01 --end-date 2025-06-30 --export-markdown
```

## 5. 复试展示建议

- 先展示研究问题（例如“未来收益率是否受动量与波动率影响”）
- 再展示假设、检验方法、显著性结论
- 最后说明模型限制（例如 p 值近似方法、样本外验证待扩展）
