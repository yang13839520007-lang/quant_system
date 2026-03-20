from pathlib import Path
import sys

# 把项目根目录加入 Python 路径
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.settings import (
    DATA_DIR,
    REPORT_DIR,
    INITIAL_CASH,
    COMMISSION_RATE,
    STAMP_TAX_RATE,
    LOT_SIZE,
    SHORT_MA,
    LONG_MA,
)
from core.loader import load_daily_csv
from factors.indicators import add_moving_averages
from strategies.ma_cross import generate_ma_cross_signals
from backtest.engine import run_single_stock_backtest


def main():
    file_name = "sh.600121.csv"   # 改成你真实存在的CSV文件名

    file_path = DATA_DIR / file_name
    df = load_daily_csv(file_path)
    df = add_moving_averages(df, SHORT_MA, LONG_MA)
    df = generate_ma_cross_signals(df)

    summary, trade_df, equity_df = run_single_stock_backtest(
        df=df,
        initial_cash=INITIAL_CASH,
        commission_rate=COMMISSION_RATE,
        stamp_tax_rate=STAMP_TAX_RATE,
        lot_size=LOT_SIZE,
    )

    signal_df = df[[
        "date", "code", "close", "ma_short", "ma_long", "buy_signal", "sell_signal"
    ]].copy()

    signal_out = REPORT_DIR / f"{file_name}_signals.csv"
    trade_out = REPORT_DIR / f"{file_name}_trades.csv"
    equity_out = REPORT_DIR / f"{file_name}_equity.csv"

    signal_df.to_csv(signal_out, index=False, encoding="utf-8-sig")
    trade_df.to_csv(trade_out, index=False, encoding="utf-8-sig")
    equity_df.to_csv(equity_out, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print("单股均线策略回测结果")
    print("=" * 60)
    for k, v in summary.items():
        print(f"{k}: {v}")

    print("=" * 60)
    print("最近5笔交易：")
    if trade_df.empty:
        print("没有产生交易")
    else:
        print(trade_df.tail(5).to_string(index=False))

    print("=" * 60)
    print(f"信号文件: {signal_out}")
    print(f"交易文件: {trade_out}")
    print(f"权益文件: {equity_out}")


if __name__ == "__main__":
    main()