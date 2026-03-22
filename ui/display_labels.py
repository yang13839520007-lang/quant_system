from __future__ import annotations

import re


PAGE_LABELS = {
    "candidates": "今日候选",
    "trade_plan": "交易计划",
    "portfolio": "风控后组合",
    "open_execution": "开盘执行",
    "intraday_recheck": "盘中复检",
    "close_review": "收盘复盘",
    "next_day_management": "次日管理",
    "stage_status": "主控阶段状态",
    "summary_logs": "摘要/日志",
}


STATUS_PANEL_LABELS = {
    "trading_day": "交易日期",
    "last_refresh": "最新刷新时间",
    "reports_status": "报表准备情况",
    "orchestrator_status": "主控执行情况",
    "current_stage": "当前进度",
    "error_status": "异常提示",
    "alert_status": "提醒概览",
}


COLUMN_LABELS = {
    "trading_date": "交易日期",
    "trade_date": "交易日期",
    "date": "日期",
    "code": "证券代码",
    "name": "证券名称",
    "action": "交易动作",
    "status": "订单状态",
    "stage_status": "阶段状态",
    "stage_no": "阶段编号",
    "stage_name": "阶段名称",
    "entry_type": "入口类型",
    "entry_target": "入口目标",
    "duration_sec": "耗时(秒)",
    "reused": "复用",
    "repaired": "修复",
    "error_message": "错误信息",
    "portfolio_rank": "组合排名",
    "execution_rank": "执行优先级",
    "order_shares": "委托数量（下单股数）",
    "order_price": "委托价格（下单价）",
    "order_type": "委托类型",
    "filled_shares": "成交股数",
    "avg_fill_price": "成交均价",
    "entry_price": "计划买入价",
    "stop_loss": "止损价格（防守价）",
    "target_price": "目标价格（止盈价）",
    "suggested_shares": "建议股数",
    "suggested_position_pct": "建议仓位占比",
    "expected_loss_amt": "预期亏损",
    "expected_profit_amt": "预期收益",
    "score": "评分",
    "heat_level": "热度级别",
    "risk_review_passed": "风控是否通过",
    "risk_review_note": "风控说明",
    "risk_review_time": "风控时间",
    "management_action": "管理动作",
    "management_priority_score": "管理优先级",
    "action_reason": "动作原因",
    "filled_qty": "成交数量",
    "close_price": "收盘价",
    "open_price": "开盘价",
    "high_price": "最高价",
    "low_price": "最低价",
    "prev_close": "昨收价",
    "market_change_pct": "涨跌幅(%)",
    "unrealized_pnl_amt": "浮盈亏",
    "unrealized_pnl_pct": "浮盈亏(%)",
    "stop_loss_gap_pct": "距止损(%)",
    "target_gap_pct": "距目标(%)",
    "position_status": "持仓状态",
    "next_day_action": "次日动作",
    "snapshot_mode": "快照模式",
    "snapshot_quality": "快照质量",
    "source_position_type": "仓位来源类型",
    "source_file": "来源文件",
    "review_time": "复盘时间",
    "hold_qty": "持仓数量",
    "available_qty": "可用数量",
    "cost_price": "成本价",
    "hold_days": "持仓天数",
    "overall_status": "总体状态",
    "acceptance_status": "验收状态",
    "run_mode_label": "运行模式",
    "reuse_audit_status": "复用审计状态",
    "production_mode_label": "生产模式",
}


FIELD_EXPLANATIONS = {
    "order_price": "实际提交委托时使用的价格，通常用于说明本次下单打算按什么价位成交。",
    "order_shares": "实际提交委托时的股数，用来表示这笔订单准备买入或卖出的数量。",
    "action": "这条记录对应的交易方向或处理动作，例如买入、卖出、持有或观察。",
    "order_type": "委托单采用的价格方式，例如限价或市价。",
    "status": "订单或记录当前所处状态，用于判断是否已执行、已跳过或失败。",
    "entry_price": "策略计划中的买入参考价，不代表一定已经成交。",
    "stop_loss": "用于防守的止损价格，价格接近这里时通常需要提高警惕。",
    "target_price": "计划中的止盈目标价格，价格接近这里时可关注兑现收益。",
    "suggested_shares": "按当前计划和资金约束推导出的建议下单股数。",
    "suggested_position_pct": "该标的建议占用的仓位比例，用于衡量仓位轻重。",
    "risk_review_passed": "这条计划是否通过风控检查，便于快速区分可执行与需复核项目。",
}


PAGE_FIELD_GUIDES = {
    "trade_plan": (
        "action",
        "entry_price",
        "stop_loss",
        "target_price",
        "suggested_shares",
        "suggested_position_pct",
    ),
    "portfolio": (
        "action",
        "entry_price",
        "stop_loss",
        "target_price",
        "suggested_shares",
        "suggested_position_pct",
        "risk_review_passed",
    ),
    "open_execution": (
        "action",
        "order_type",
        "order_price",
        "order_shares",
        "status",
    ),
}


STATUS_LABELS = {
    "NOT_RUN": "未运行",
    "RUNNING": "运行中",
    "SKIPPED": "已跳过",
    "SUCCESS": "成功",
    "FAILED": "失败",
    "PENDING": "待处理",
    "PASS": "通过",
    "REJECT": "拒绝",
    "SUCCESS_EXECUTED": "执行成功",
    "SUCCESS_REUSED": "复用成功",
    "SUCCESS_REPAIRED": "修复成功",
    "PASS_REALTIME_MODE": "通过实时模式",
    "FULL_REALTIME_RECOMPUTE": "全量实时重算",
    "FULL_REALTIME_READY": "实时就绪",
    "NONCORE_FORCE_EXECUTE": "非核心强制执行",
    "BUY": "买入",
    "SELL": "卖出",
    "LIMIT": "限价",
    "MARKET": "市价",
    "WARNING": "警告",
    "UNKNOWN": "未知",
    "PAUSED_FALLBACK": "暂停回退",
    "REPLAY_PROXY": "回放代理",
    "PLAN_FALLBACK": "计划回退",
}


MESSAGE_LABELS = {
    "file_missing": "文件不存在/尚未生成",
    "waiting_load": "等待加载",
    "waiting_logs": "等待主控日志输出",
    "loaded_summary": "已加载摘要",
    "no_summary": "暂无摘要",
    "search_label": "关键字筛选",
    "search_placeholder": "输入代码、状态或关键词筛选当前表格",
    "filter_state_template": "显示 {visible} / {total} 行",
    "summary_label": "文字摘要",
    "summary_page_title": "主控摘要",
    "log_page_title": "主控运行日志",
    "stage_status_title": "主控阶段状态",
    "summary_logs_tab": "摘要/日志",
    "run_today": "运行今日主控",
    "refresh_data": "刷新数据",
    "open_reports": "打开报表目录",
    "open_logs": "打开日志目录",
    "refresh_pending": "刷新任务仍在进行，本次刷新请求已排队。",
    "refreshing_prefix": "正在后台刷新数据",
    "refresh_done_template": "数据刷新完成，用时 {duration_ms} ms",
    "refresh_failed": "刷新失败，请检查日志。",
    "runner_idle": "空闲",
    "runner_running_template": "运行中 / {trading_date}",
    "runner_started": "主控已启动",
    "runner_finished": "已结束",
    "alert_none": "无提醒",
    "reports_missing_mode": "未找到报表目录，已进入空白监控模式",
    "reports_ready_template": "已就绪 {present}/{total} 份关键报表，共 {file_count} 个文件；最近更新：{recent}",
    "summary_status_prefix": "主控执行情况：",
    "open_directory_failed": "打开目录失败",
    "open_directory_failed_message": "无法打开目录：{path}\n请确认系统资源管理器可用，或手动打开该路径。",
    "runner_start_failed": "主控无法启动",
    "runner_failed_dialog": "主控运行失败",
    "runner_failed_detail": "{message}\n请检查摘要/日志页中的 stdout/stderr 和阶段状态。",
}


STATUS_COLUMNS = {
    "status",
    "stage_status",
    "overall_status",
    "acceptance_status",
    "run_mode_label",
    "reuse_audit_status",
    "production_mode_label",
    "order_type",
    "action",
    "next_day_action",
    "management_action",
    "snapshot_mode",
    "snapshot_quality",
    "source_position_type",
    "position_status",
}


STATUS_TOKEN_PATTERN = re.compile(r"\b[A-Z][A-Z0-9_]+\b")


def get_page_label(key: str, default: str | None = None) -> str:
    return PAGE_LABELS.get(key, default or key)


def get_status_panel_label(key: str, default: str | None = None) -> str:
    return STATUS_PANEL_LABELS.get(key, default or key)


def get_column_label(column_name: str) -> str:
    return COLUMN_LABELS.get(column_name, column_name)


def get_column_tooltip(column_name: str) -> str:
    label = get_column_label(column_name)
    explanation = FIELD_EXPLANATIONS.get(column_name, "")
    if not explanation:
        return label
    return f"{label}\n原字段：{column_name}\n说明：{explanation}"


def get_message(key: str, default: str = "") -> str:
    return MESSAGE_LABELS.get(key, default)


def get_page_field_guide(page_key: str, available_columns: list[str] | tuple[str, ...]) -> str:
    guide_fields = PAGE_FIELD_GUIDES.get(page_key, ())
    if not guide_fields:
        return ""

    available_set = {str(column) for column in available_columns}
    parts: list[str] = []
    for field_name in guide_fields:
        if field_name not in available_set:
            continue
        label = get_column_label(field_name)
        explanation = FIELD_EXPLANATIONS.get(field_name)
        if not explanation:
            continue
        parts.append(f"{label}（{field_name}）：{explanation}")
    return " | ".join(parts)


def format_status_code(code: str) -> str:
    raw = str(code).strip()
    if not raw:
        return raw

    label = STATUS_LABELS.get(raw.upper())
    if not label:
        return raw
    return f"{label}（{raw}）"


def format_status_text(text: str) -> str:
    raw = str(text).strip()
    if not raw:
        return raw

    if raw.upper() in STATUS_LABELS:
        return format_status_code(raw)

    if " / " in raw:
        return " / ".join(_format_possible_status_token(part.strip()) for part in raw.split(" / "))

    return STATUS_TOKEN_PATTERN.sub(lambda match: format_status_code(match.group(0)), raw)


def format_column_value(value: object, column_name: str = "") -> str:
    if isinstance(value, bool):
        return "是" if value else "否"

    text = str(value)
    normalized_column = column_name.lower().strip()
    if normalized_column in STATUS_COLUMNS:
        return format_status_text(text)

    if text.upper() in STATUS_LABELS:
        return format_status_code(text)

    return text


def _format_possible_status_token(token: str) -> str:
    if token.upper() in STATUS_LABELS:
        return format_status_code(token)
    return token
