# A股半自动交易辅助系统使用手册（Windows）

## 1. 这套系统要完成什么

这套系统的核心目标不是“自动下单”，而是把投研到交易准备的全流程做成可复用、可审计、可回放的闭环。

它主要完成 3 件事：

1. 生成可解释的交易建议  
   从行情、因子、策略、事件数据中生成 `BUY/SELL/WATCH` 信号，并给出理由、置信度、风险命中说明。

2. 在下单前做风险与收益验证  
   通过风控规则、回测结果、组合优化和调仓建议，先验证“能不能做、怎么做更稳”。

3. 在下单后形成复盘与治理闭环  
   人工下单后回写执行，生成复盘统计；同时用运维看板监控公告连接器、SLA、告警、失败重放。

## 2. 最重要的原则（先看这个）

1. **系统是半自动，最终下单由人执行**  
   不接券商交易执行，不会自动报单。

2. **风控优先于收益**  
   出现 `CRITICAL` 风险、连接器严重告警、数据新鲜度不足时，应先处理风险再考虑收益。

3. **参数和数据要可追溯**  
   每次运行都应保留参数、信号、回写、回放结果，保证后续复盘和合规审计。

4. **不要只看单次回测收益**  
   必须结合回撤、夏普、交易次数、阻断数量、执行跟随率一起看。

## 3. 使用前准备（Windows）

## 3.1 环境要求

- Windows 10/11
- Python 3.11+（已加入 PATH）
- 建议可访问 `akshare` 数据源

## 3.2 一键启动（推荐）

项目根目录已有一键脚本：`start_system_windows.bat`

双击它会自动完成：

1. 切换到项目目录
2. 检查并创建 `.venv`
3. 检查并创建 `.env`
4. 必要时安装依赖 `pip install -e .[dev]`
5. 启动后端服务（新开终端窗口）
6. 自动打开 3 个页面  
   - `http://127.0.0.1:8000/ui/`  
   - `http://127.0.0.1:8000/trading/workbench`  
   - `http://127.0.0.1:8000/ops/dashboard`

可选参数：

- `start_system_windows.bat --dry-run`：只打印步骤，不执行
- `start_system_windows.bat --no-browser`：只拉起后端，不自动开网页

## 3.3 手动启动（备用）

在项目目录执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\run_api.ps1
```

## 3.4 关闭系统

关闭名为 `Trading Assistant API` 的终端窗口，或在该窗口按 `Ctrl + C`。

---

## 4. 前端总览与页面关系

系统前端有 3 个页面：

1. 主界面（总入口）：`/ui/`
2. 投研交易工作台（策略、结果、执行）：`/trading/workbench`
3. 运维 + 事件治理看板：`/ops/dashboard`

页面跳转关系：

- 主界面可以进入另外两个页面
- 投研页面头部可跳主界面、运维页面
- 运维页面头部可跳主界面、投研页面
- 投研页面内部结果页可一键跳转到策略页或执行页

---

## 5. 主界面（`/ui/`）怎么用

主界面作用是“导航 + 快速状态”：

1. 显示系统状态（来自 `/health`）
2. 显示策略数量（来自 `/strategies`）
3. 提供流程化入口：
   - 进入投研工作台
   - 进入运维看板
   - 直达投研子页与运维回放工作台

建议把主界面作为每天开盘前的起点页。

---

## 6. 投研交易工作台（`/trading/workbench`）详解

该页是你日常最常用页面，分 3 个子页签：

1. 策略与参数页
2. 结果可视化页
3. 交易准备单与执行回写页

## 6.1 策略与参数页（怎么配、怎么跑）

### 6.1.1 基础配置区域

- `symbol`：单标的运行对象（信号、回测）
- `symbols`：多标的列表（研究工作流）
- `start_date/end_date`：统一时间窗口
- `strategy_name`：策略选择
- `industry`：行业字段（可给风控、行业暴露参考）
- `enable_event_enrichment`：启用事件增强
- `event_lookback_days`：事件回看窗口
- `event_decay_half_life_days`：事件衰减速度
- `enable_small_capital_mode`：启用小资金模式（可交易过滤 + 成本覆盖检查）
- `small_capital_principal`：小资金本金（如 2000 元）
- `small_capital_min_expected_edge_bps`：最低安全边际（预期边际需覆盖交易成本+安全边）
- 小资金模板按钮：`套用 2000 档 / 5000 档 / 8000 档`，可一键填充策略与关键参数
- `enable_fundamental_enrichment`：启用财报基本面增强（建议保持开启）
- `fundamental_max_staleness_days`：财报最大陈旧天数，超出后评分会衰减

### 6.1.2 策略参数（动态）

系统会根据 `strategy_name` 自动展示参数输入项，字段名保持英文，便于与后端 API 对齐。

### 6.1.3 信号风控上下文（可选）

可选启用：

- `current_position`：当前持仓（数量、可卖数量、成本、市值、最近买入日）
- `portfolio_snapshot`：组合总资产、现金、峰值、当前回撤

作用：让风险引擎在“真实持仓上下文”下判断，而不是只看裸信号。

### 6.1.4 回测与研究参数

- `initial_cash`
- `commission_rate`
- `slippage_rate`
- `min_commission_cny`
- `stamp_duty_sell_rate`
- `transfer_fee_rate`
- `lot_size`
- `optimize_portfolio`
- `max_single_position`
- `max_industry_exposure`
- `target_gross_exposure`
- `industry_map`（每行 `代码=行业`）

### 6.1.5 运行按钮

- `运行信号生成` -> `POST /signals/generate`
- `运行回测` -> `POST /backtest/run`
- `运行研究工作流` -> `POST /research/run`
- `一键全跑`：按顺序执行信号、回测、研究

补充：

- 页面会实时显示“请求体预览”，用于核对参数
- 页面输入会被本地缓存（浏览器 localStorage），刷新后可恢复

## 6.2 如何选策略（非常重要）

系统内置 6 种策略：

1. `trend_following`（趋势跟随）  
   适合：趋势明确、波段行情。  
   核心：`MA20/MA60` 趋势 + `ATR` 退出带。

2. `mean_reversion`（均值回归）  
   适合：震荡市、偏短线回归。  
   核心：`zscore20` 偏离回归 + 流动性门槛。

3. `multi_factor`（多因子）  
   适合：多标的筛选、组合研究。  
   核心：动量、质量、低波动、流动性 + 财报评分加权打分。

4. `sector_rotation`（行业轮动）  
   适合：板块切换明显阶段。  
   核心：行业强弱 + 风险偏好状态 + 动量确认。

5. `event_driven`（事件驱动）  
   适合：公告/事件信息密集时段。  
   核心：`event_score` 与 `negative_event_score` 触发。

6. `small_capital_adaptive`（小资金自适应）  
   适合：长期本金低于 1 万元、希望控制换手和费用拖累的账户。  
   核心：在信号阶段就做“一手可买性 + 集中度 + 动态仓位”联动，避免生成不可执行 BUY。

实操建议：

1. 初次上手先用 `trend_following` 跑单标的，便于理解信号与回测关系。
2. 想做组合时优先 `multi_factor + optimize_portfolio`。
3. 事件流质量稳定后，再提高 `event_driven` 权重。
4. 参数调优要逐步改，避免一次改太多无法解释收益变化。
5. 财报增强建议默认开启；若关闭，仅剩技术/事件维度，建议降低仓位并加强人工复核。
6. 若本金长期低于 1 万元：优先 `small_capital_adaptive`，并开启 `enable_small_capital_mode`，先保证“可执行”再追求收益弹性。

---

## 7. 结果可视化页（`#results`）详解

这个页面用于“看懂结果、做出准备动作”。

## 7.1 顶部 KPI

核心指标：

- 信号数量
- 买入建议数
- 阻断数量
- 回测收益/最大回撤/夏普
- 研究候选数量

用途：先判断本轮结果是否值得深入看。

## 7.2 信号与风控联动

左侧表：信号与交易准备单  
右侧表：风控命中明细

当前信号表新增“财报评分”列（0~1），用于快速识别买入建议是否有基本面支撑。

联动规则：

1. 点击左表某行
2. 右侧自动显示该信号的风控命中、建议动作、免责声明

## 7.3 研究工作流与组合优化

研究候选表展示多标的结果。  
组合优化表展示 `optimized_portfolio.weights`。

说明：

- 这里只展示“目标权重建议”，不是执行指令
- 后续调仓指令由 `rebalance plan` 生成
- 研究候选会携带 `fundamental_score`，组合优化会把技术动量与财报评分联合用于候选预期收益估计

## 7.4 K线/价格走势叠加信号点位（`/market/bars`）

用途：把“策略信号”放回价格路径里验证直觉。

操作步骤：

1. 点 `同步策略参数`（自动带入 `symbol/start_date/end_date`）
2. 点 `加载K线并叠加信号`
3. 观察 K 线图上的标记点：
   - 绿色：`BUY`
   - 红色：`SELL`
   - 橙色：`WATCH`
4. 结合下方 bar 表核对具体日期和 OHLC

注意：

- `limit` 最大 200（接口限制）
- 叠加信号来自“最近一次信号生成结果”

## 7.5 组合权重可视化 + 调仓建议联动（`/portfolio/rebalance/plan`）

左侧图：

- 目标权重图（按 symbol）
- 行业暴露图（按 industry）

右侧调仓区：

1. 填 `total_equity`、`lot_size`
2. 填当前持仓 `current_positions`，每行：
   `symbol,quantity,last_price`
3. 点 `生成调仓建议`
4. 查看输出表：
   - `side`
   - `target_weight`
   - `delta_weight`
   - `quantity`
   - `estimated_notional`

辅助按钮：

- `同步默认资金/手数`：从回测参数同步
- `用买入信号构造持仓样例`：快速生成示例持仓文本

## 7.6 回测曲线与明细

包含：

- `equity_curve` 图
- 指标明细表（收益、回撤、夏普、交易次数、胜率、阻断）
- 成交记录表（每笔动作、价格、数量、费用、阻断原因）

你应重点看：

1. 回撤是否可承受
2. 夏普是否稳定
3. 阻断是否过多（可能参数过激或风控条件过严）
4. 小资金提示是否频繁触发（说明当前标的价格/成本结构不适合账户规模）

---

## 8. 交易准备单与执行回写页（`#execution`）详解

本页解决“从建议到执行到复盘”的闭环。

## 8.1 交易准备单

来自最近一次信号结果。  
每行可点 `填入执行单`，自动把 `signal_id/symbol/side/date` 带入执行表单。

## 8.2 信号记录库（`/replay/signals`）

用于查询历史信号决策记录，可按 `symbol` 和 `limit` 过滤。

## 8.3 执行回写（`/replay/executions/record`）

填写人工成交结果：

- `signal_id`
- `symbol`
- `execution_date`
- `side`
- `quantity`
- `price`
- `fee`
- `note`

提交后会写入执行记录库。

## 8.4 执行复盘报表（`/replay/report`）

可按时间区间与标的查看：

- 样本数
- 跟随率（执行动作是否跟随信号）
- 平均延迟天数
- 平均滑点（当前实现里基线为 0）

---

## 9. 运维 + 事件治理看板（`/ops/dashboard`）详解

这个页面不直接给买卖建议，它保障“数据链路和治理质量可用”。

## 9.1 顶部 KPI

包括：

- 作业运行次数
- 调度器 SLA 违约数
- 连接器 SLA 严重告警
- 连接器未恢复数
- 连接器升级中未恢复数
- 未确认严重告警
- 事件覆盖标的数

## 9.2 主要板块

1. 事件覆盖总览  
   看事件量、正负事件、来源覆盖与日度分布。

2. 连接器健康与积压  
   看最近运行状态、新鲜度、待重试、死信、checkpoint。

3. 多源矩阵健康  
   看 active/standby 源、健康分、有效分、连续失败、最后成功时间。

4. SLA 违约与升级状态机  
   一块看“违约列表”，一块看“未恢复状态（含升级级别）”。

5. NLP 规则集与漂移监控  
   看活跃规则版本、漂移趋势、快照告警等级。

6. SLO 消耗率历史  
   看连接器与 NLP 的 burn rate 曲线和时间表。

7. 作业与调度器 SLA  
   看近期作业运行和调度器违约明细。

8. 未确认告警 + 值班回调时间线  
   看当前告警积压和 oncall 回调闭环。

9. 回放处理工作台（重点）  
   失败记录筛选、编辑 payload、保存修复、手动重放、修复后重放。

## 9.3 回放处理工作台操作步骤（建议照做）

1. 选择 `连接器`
2. 选择状态过滤（`PENDING/DEAD/REPLAYED/ALL`）
3. 可加错误关键词过滤
4. 点 `加载失败列表`
5. 勾选需要处理的失败记录
6. 点某条 `编辑`，查看 `raw_record` 和 `event`
7. 修改 JSON（必须是 JSON 对象）
8. 先点 `保存修复`（只修复不重放）或直接点 `保存并重放选中项`
9. 看“重放结果”表，确认成功/失败/死信数量
10. 再回看顶部 KPI 是否恢复

---

## 10. 页面与后端是如何联动的

## 10.1 交易主链路

1. 策略参数页  
   `POST /signals/generate` -> 信号与准备单（含财报评分与财报风控门槛）

2. 结果页  
   `POST /backtest/run` -> 回测图与指标  
   `POST /research/run` -> 多标的研究与优化权重  
   `GET /market/bars` -> K线叠加信号  
   `POST /portfolio/rebalance/plan` -> 调仓建议

3. 执行页  
   `POST /replay/executions/record` -> 回写执行  
   `GET /replay/report` -> 复盘统计

## 10.2 运维治理链路

1. 看板汇总：`GET /metrics/ops-dashboard`
2. 连接器健康：`/events/connectors/overview`、`/events/connectors/source-health`
3. SLA：`/events/connectors/sla`、`/events/connectors/sla/states`、`/events/connectors/sla/states/summary`
4. 重放修复：`/events/connectors/failures`、`/events/connectors/failures/repair`、`/events/connectors/replay/manual`、`/events/connectors/replay/repair`

---

## 11. 推荐的一次完整使用流程（从 0 到 1）

以下是一套可直接复用的日常流程：

1. 双击 `start_system_windows.bat`
2. 打开主界面，确认系统状态正常
3. 进入投研工作台，先做单标的试跑：
   - 策略选 `trend_following`
   - 填 `symbol=000001`
   - 设好日期
   - 点 `运行信号生成`
4. 在结果页检查：
   - 信号动作和风控命中
   - K线叠加信号是否合理
5. 点 `运行回测`，检查收益/回撤/夏普
6. 切回策略页，填多标的，点 `运行研究工作流`
7. 在结果页查看：
   - 候选标的
   - 优化权重
   - 行业暴露
8. 填当前持仓，生成调仓建议
9. 跳转执行页，按建议人工下单后回写执行
10. 查看复盘报表的跟随率与延迟
11. 打开运维看板，确认连接器与 SLA 正常；若失败积压，进入回放工作台处理

---

## 12. 你最需要长期盯住的指标

1. 风险维度  
   最大回撤、CRITICAL 命中数、阻断信号数

2. 收益质量维度  
   年化收益、夏普、收益稳定性（不要只看单次区间）

3. 执行偏差维度  
   跟随率、平均延迟、手工执行偏差

4. 治理健康维度  
   连接器新鲜度、待重试/死信、SLA 升级状态、NLP 漂移告警

5. 财报质量维度  
   财报覆盖率（有无快照）、`fundamental_score`分布、低分阻断/预警数量

6. 小资金适配维度  
   小资金阻断次数、最小手数可交易覆盖率、成本占预期边际比例

---

## 13. 常见问题与排查

## 13.1 页面打不开

1. 先访问 `http://127.0.0.1:8000/health`
2. 若不通，检查 API 窗口是否仍在运行
3. 若 8000 端口被占用，先关占用进程再重启

## 13.2 没有数据或信号一直是 WATCH

1. 检查日期区间是否过短
2. 检查标的代码是否有效
3. 检查策略参数是否过严
4. 检查事件增强是否开启但事件源无数据
5. 检查财报增强是否开启；若开启但数据源无财报快照，系统会回退为技术/事件驱动

## 13.3 回测结果差异很大

1. 检查手续费、滑点、手数设置是否一致
2. 检查是否启用了事件增强（会改变信号）
3. 检查是否使用了不同策略参数

## 13.4 调仓建议无法生成

1. 先确保研究结果里有 `optimized_portfolio.weights`
2. `current_positions` 格式必须是 `symbol,quantity,last_price`
3. 确保 `total_equity > 0`、`lot_size >= 1`

## 13.5 401/403 权限错误

如果启用了 `AUTH_ENABLED=true`：

1. 在投研页面头部配置 `X-API-Key`
2. 确认该 key 对应角色具备目标接口权限

---

## 14. 最后建议（实盘前）

1. 先用 1-2 个标的做完整闭环，打通流程再扩容
2. 每次只改一组参数，保留前后对比记录
3. 把“收益判断”与“治理判断”分开做，任何一方异常都不要急于加仓
4. 每周固定做一次复盘：信号质量 + 执行偏差 + SLA 健康

---

如需把本手册升级为“团队 SOP 版本”，下一步建议补充：

1. 你的账户规模分层（小/中/大资金）的参数模板
2. 不同行情状态（趋势/震荡/高波动）下的策略切换规则
3. 你的人工下单纪律（撤单规则、滑点容忍、仓位上限）
