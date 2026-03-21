# A股量化交易监控终端

## 安装依赖

建议使用 Python 3.11 及以上版本；如果是更早版本，`requirements_ui.txt` 已补充 `tomli` 回退依赖。

```powershell
cd C:\quant_system
python -m pip install -r requirements_ui.txt
```

## 启动方式

```powershell
cd C:\quant_system
python -m ui.main
```

默认配置文件位于 `ui/ui_config.toml`。第二轮已将路径统一收敛到配置层，默认 `project_root = ".."`，不再把项目根目录硬编码在 UI 代码里。

## 目录说明

- `ui/main.py`: UI 启动入口。
- `ui/main_window.py`: 主窗口、按钮、标签页、后台刷新、主控运行交互。
- `ui/config.py`: 配置加载与页面定义。
- `ui/ui_config.toml`: UI 配置文件，集中管理路径与刷新参数。
- `ui/data_loader.py`: 容错读取 `reports` 目录下的 CSV/TXT/JSON。
- `ui/refresh_worker.py`: 后台刷新 worker，避免主线程因读报表而阻塞。
- `ui/alert_manager.py`: 提醒评估与桌面通知中心。
- `ui/orchestrator_runner.py`: 后台线程运行现有主控脚本并回传 stdout/stderr。
- `ui/orchestrator_entry.py`: UI 子进程调用现有 `TradingDayOrchestratorManager` 的桥接入口。
- `ui/log_manager.py`: 文件日志与内存日志管理。
- `ui/widgets/status_panel.py`: 顶部状态面板。
- `ui/widgets/table_page.py`: 报表表格页，带排序、过滤和多行摘要。
- `ui/widgets/summary_viewer.py`: 主控摘要查看器。
- `ui/widgets/log_viewer.py`: 运行日志查看器。
- `requirements_ui.txt`: UI 依赖清单。

## 主要功能

- 正式桌面主窗口，可在无报表时正常启动。
- 顶部状态区展示交易日、最近刷新时间、`reports` 状态、主控状态、当前阶段、错误状态、告警状态。
- 7 个业务标签页 + 1 个摘要/日志标签页。
- 容错读取以下关键报表并降级展示：
  - `daily_candidates_top20.csv`
  - `daily_trade_plan_top10.csv`
  - `daily_portfolio_plan_risk_checked.csv`
  - `daily_open_execution_orders.csv`
  - `daily_intraday_recheck_orders.csv`
  - `daily_close_review.csv`
  - `daily_next_day_management.csv`
- 兼容显示相关 TXT 摘要，缺失时只提示，不导致窗口崩溃。
- 表格支持排序、关键字搜索/筛选，并显示过滤后行数。
- 支持从界面运行现有主控、刷新数据、打开 `reports` 和日志目录。
- 摘要/日志页显示主控摘要、阶段状态表和主控运行输出。
- 提醒中心统一处理：
  - 主控运行完成提醒
  - 可执行买入信号提醒
  - 止损/止盈关键词提醒
  - 盘中改判提醒
  - 报表读取异常提醒

## 第二轮强化点

- 路径统一收敛到配置层，不再依赖 UI 代码内固定项目根路径。
- 报表刷新改为后台线程，降低主线程阻塞风险。
- 提醒逻辑从数据加载层剥离，集中到 `ui/alert_manager.py`。
- 表格页补充过滤计数、列宽策略、只读行选中体验。
- 主控启动失败时补充解释器路径、桥接入口路径等可读错误提示。
- `reports` 目录不存在时，会明确提示“空白监控模式”。

## 已知限制

- 当前版本仍然是正式 MVP，不包含完整托盘菜单、声音提醒配置和主题切换面板。
- 主控运行日志当前仍是文本流展示，尚未结构化到阶段级日志模型。
- 表格筛选为全文本匹配，暂未提供列级高级过滤器。
- 当前没有做 exe 打包脚本，仍以 Python 环境启动。

## Windows 最小验收流程

### 1. 安装依赖

```powershell
cd C:\quant_system
python -m pip install -r requirements_ui.txt
```

### 2. 启动 UI

```powershell
cd C:\quant_system
python -m ui.main
```

### 3. 验证基础界面

- 确认主窗口能打开。
- 确认顶部状态区能显示交易日、最近刷新时间、`reports` 状态。
- 确认 8 个标签页都能切换。

### 4. 验证无报表启动

可临时把 `ui/ui_config.toml` 中的 `reports_dir` 改成一个不存在的目录，例如：

```toml
reports_dir = "temp/empty_reports_for_ui_test"
```

然后重新执行：

```powershell
python -m ui.main
```

确认：

- 窗口仍能正常打开。
- 各页面不会崩溃。
- 顶部错误状态会提示空白监控模式。

验证后把 `reports_dir` 改回 `reports`。

### 5. 验证表格刷新与过滤

- 打开“今日候选”或“交易计划”页。
- 在搜索框输入股票代码或关键字。
- 确认表格行数变化，右侧显示“显示 X / Y 行”。
- 点击列头，确认可排序。

### 6. 验证主控运行与日志输出

- 点击“运行今日主控”。
- 切到“摘要/日志”页。
- 确认日志窗口持续滚动显示 stdout/stderr。
- 确认运行期间主窗口没有卡死，仍可切换标签页。

### 7. 验证运行完成后的状态回写

- 等主控结束。
- 确认会出现运行完成提醒。
- 确认“主控阶段状态”表刷新。
- 确认“主控摘要”文本刷新。

## 下一步扩展建议

- 系统托盘：最小化到托盘、托盘菜单、托盘状态灯。
- 声音提醒：主控完成、失败、买入信号的可配置声音通知。
- 自动刷新增强：支持开关、频率选择、仅交易时段刷新。
- 打包 exe：补充 `pyinstaller` 或 `nuitka` 打包脚本。
- 主题美化：状态颜色、表格高亮、浅色/深色主题和品牌化图标。
