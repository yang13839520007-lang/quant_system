# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 15:52:57 2026

@author: DELL
"""

from pathlib import Path

BASE_DIR = Path(r"C:\quant_system")
DATA_DIR = BASE_DIR / "stock_data_5years"
REPORT_DIR = BASE_DIR / "reports"

REPORT_DIR.mkdir(exist_ok=True)

INITIAL_CASH = 100000.0
COMMISSION_RATE = 0.0003
STAMP_TAX_RATE = 0.001
LOT_SIZE = 100

SHORT_MA = 5
LONG_MA = 10

# =========================
# 组合层参数
# =========================
PORTFOLIO_MAX_STOCKS = 5
PORTFOLIO_CAPITAL = 1_000_000          # 总资金，先写死，后面再改成可传参
PORTFOLIO_MIN_TOTAL_POSITION = 0.30    # 最低目标总仓
PORTFOLIO_MAX_TOTAL_POSITION = 0.60    # 最高目标总仓
PORTFOLIO_MAX_SINGLE_POSITION = 0.15   # 单票硬上限
PORTFOLIO_MAX_HOT_COUNT = 2            # 偏热/过热票最多数量
PORTFOLIO_MAX_GROWTH_BOARD_COUNT = 2   # 300/688 最多数量

PORTFOLIO_ACTION_FACTOR = {
    "正常跟踪": 1.00,
    "小仓跟踪": 0.80,
    "观察": 0.00,
}

PORTFOLIO_HEAT_FACTOR = {
    "正常": 1.00,
    "偏热": 0.88,
    "过热": 0.72,
}

# =========================
# 组合风控复核参数
# =========================
PORTFOLIO_RISK_METADATA_PATH = r"C:\quant_system\reports\stock_risk_metadata.csv"

PORTFOLIO_MAX_INDUSTRY_POSITION = 0.25      # 单行业最大仓位
PORTFOLIO_MAX_STYLE_POSITION = 0.30         # 单风格最大仓位
PORTFOLIO_MAX_HOT_POSITION = 0.12           # 偏热/过热合计最大仓位
PORTFOLIO_MAX_GROWTH_POSITION = 0.20        # 300/688 合计最大仓位

PORTFOLIO_MAX_EXPECTED_LOSS_PCT = 0.04      # 组合统一止损时，总亏损不超过总资金4%
PORTFOLIO_UNKNOWN_INDUSTRY = "未知行业"
PORTFOLIO_UNKNOWN_STYLE = "未知风格"

# =========================
# 执行层参数
# =========================
EXECUTION_KEEP_CORE_COUNT = 3

EXECUTION_NORMAL_BUY_LOW_PCT = 0.985
EXECUTION_NORMAL_BUY_HIGH_PCT = 1.015
EXECUTION_NORMAL_CHASE_PCT = 1.03
EXECUTION_NORMAL_DISCOUNT_PCT = 0.98

EXECUTION_HOT_BUY_LOW_PCT = 0.985
EXECUTION_HOT_BUY_HIGH_PCT = 1.008
EXECUTION_HOT_CHASE_PCT = 1.015
EXECUTION_HOT_DISCOUNT_PCT = 0.975

EXECUTION_OVERHEAT_BUY_LOW_PCT = 0.980
EXECUTION_OVERHEAT_BUY_HIGH_PCT = 1.000
EXECUTION_OVERHEAT_CHASE_PCT = 1.005
EXECUTION_OVERHEAT_DISCOUNT_PCT = 0.970

EXECUTION_STOPLOSS_BUFFER_PCT = 1.01

# =========================
# 开盘动态执行参数
# =========================
EXECUTION_DISCOUNT_BUY_RATIO = 0.50   # 低吸区只买计划仓位的50%
EXECUTION_CHASE_BUY_RATIO = 0.50      # 追价区只买计划仓位的50%
EXECUTION_MIN_ORDER_LOTS = 1          # 最少1手

# =========================
# 盘中二次确认参数
# =========================
INTRADAY_RECHECK_REBOUND_RATIO = 0.50      # 高开放弃后回落再接，只买剩余计划仓位的50%
INTRADAY_RECHECK_RECOVERY_RATIO = 0.50     # 低开破结构后修复再接，只买剩余计划仓位的50%
INTRADAY_RECHECK_FILL_RATIO = 1.00         # 开盘已部分成交，二次确认允许补满剩余仓位
INTRADAY_RECHECK_RECOVERY_CONFIRM_PCT = 1.01   # 修复确认线 = skip_if_open_lt * 1.01

# =========================
# 收盘复盘层参数
# =========================
CLOSE_REVIEW_STOPLOSS_ALERT_BUFFER_PCT = 1.02   # 收盘价 <= 止损价*1.02，记为接近止损
CLOSE_REVIEW_TARGET_ALERT_BUFFER_PCT = 0.98     # 收盘价 >= 目标价*0.98，记为接近止盈
CLOSE_REVIEW_RETAIN_WATCH_MAX_DISTANCE_PCT = 0.03   # 空仓票若收盘距入场价不超过3%，可保留次日观察

# =========================
# 持仓续管层参数
# =========================
HOLD_REDUCE_TRIGGER_PCT = -0.02          # 浮亏达到 -2% 进入减仓观察
HOLD_STOP_TRIGGER_BUFFER_PCT = 1.01      # 收盘 <= 止损价*1.01，次日优先风控
HOLD_TAKE_PROFIT_TRIGGER_PCT = 0.03      # 浮盈达到 3% 进入止盈跟踪
HOLD_STRONG_TAKE_PROFIT_PCT = 0.06       # 浮盈达到 6% 进入强止盈跟踪
HOLD_WATCHLIST_MAX_COUNT = 20            # 次日观察池最多保留数量