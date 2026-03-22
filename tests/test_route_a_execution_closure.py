from __future__ import annotations

from pathlib import Path
import shutil
import uuid

import pandas as pd

from core.close_review_manager import CloseReviewManager
from core.open_execution_manager import OpenExecutionManager
from generate_next_day_management import NextDayManagementGenerator


def _make_case_dir() -> Path:
    path = Path("temp") / f"pytest_stage4_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    (path / "reports").mkdir(parents=True, exist_ok=True)
    return path


def _write_route_a_buy_inputs(
    base_dir: Path,
    *,
    trading_date: str,
    open_price: float,
    low_price: float,
    high_price: float,
    low_limit: float,
) -> None:
    reports = base_dir / "reports"
    pd.DataFrame(
        [
            {
                "trading_date": trading_date,
                "execution_rank": 1,
                "code": "sz.000001",
                "name": "平安银行",
                "entry_price": 10.0,
                "review_planned_shares": 1000,
                "chase_limit_price": 11.0,
                "skip_if_open_lt": 9.0,
            }
        ]
    ).to_csv(reports / "daily_execution_plan.csv", index=False, encoding="utf-8-sig")

    pd.DataFrame(
        [
            {
                "trade_date": trading_date,
                "signal_date": trading_date,
                "code": "sz.000001",
                "name": "平安银行",
                "entry_mode": "ROUTE_A_LEFT_CATCH",
                "target_buy_price_base": 10.0,
                "entry_valid": True,
                "stop_mode": "CLOSE_BELOW_MA20_EXIT_NEXT_OPEN",
                "stop_loss": 9.6,
                "target_profit_pct": 0.08,
                "shadow_threshold": 0.60,
                "route_a_signal": True,
                "target_price": 10.8,
            }
        ]
    ).to_csv(reports / "daily_trade_plan_all.csv", index=False, encoding="utf-8-sig")

    pd.DataFrame(
        [
            {
                "trade_date": trading_date,
                "code": "sz.000001",
                "name": "平安银行",
                "paused": False,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": max(open_price, low_price),
                "prev_close": 10.0,
                "high_limit": 11.0,
                "low_limit": low_limit,
                "snapshot_quality": "EXECUTION_READY",
                "source_trade_date": trading_date,
            }
        ]
    ).to_csv(reports / "market_signal_snapshot.csv", index=False, encoding="utf-8-sig")


def _run_close_review_and_next_day(
    base_dir: Path,
    *,
    trading_date: str,
    close_price: float,
    open_price: float,
    high_price: float,
    low_price: float,
    ma20: float,
    avg_fill_price: float = 100.0,
    target_profit_pct: float = 0.08,
    shadow_threshold: float = 0.60,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    reports = base_dir / "reports"
    pd.DataFrame(
        [
            {
                "execution_rank": 1,
                "code": "sz.000001",
                "name": "平安银行",
                "filled_shares": 1000,
                "avg_fill_price": avg_fill_price,
                "entry_price": avg_fill_price,
                "stop_loss": 95.0,
                "target_price": round(avg_fill_price * (1 + target_profit_pct), 4),
                "route_a_signal": True,
                "signal_date": trading_date,
                "entry_mode": "ROUTE_A_LEFT_CATCH",
                "stop_mode": "CLOSE_BELOW_MA20_EXIT_NEXT_OPEN",
                "target_profit_pct": target_profit_pct,
                "shadow_threshold": shadow_threshold,
                "action": "BUY",
                "score": 90.0,
                "heat_level": "正常",
            }
        ]
    ).to_csv(reports / "daily_open_execution_decision.csv", index=False, encoding="utf-8-sig")

    pd.DataFrame(
        [
            {
                "trade_date": trading_date,
                "code": "sz.000001",
                "name": "平安银行",
                "close_price": close_price,
                "prev_close": avg_fill_price,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "ma20": ma20,
                "snapshot_mode": "EXECUTION_READY",
                "snapshot_quality": "EXECUTION_READY",
            }
        ]
    ).to_csv(reports / "market_signal_snapshot.csv", index=False, encoding="utf-8-sig")

    CloseReviewManager(base_dir=str(base_dir)).run(trading_date=trading_date)
    NextDayManagementGenerator(base_dir=str(base_dir)).run(trading_date="2026-03-23")

    review_df = pd.read_csv(reports / "daily_close_review.csv", encoding="utf-8-sig")
    next_day_df = pd.read_csv(reports / "daily_next_day_management.csv", encoding="utf-8-sig")
    return review_df, next_day_df


def test_route_a_high_open_without_pullback_does_not_fill() -> None:
    base_dir = _make_case_dir()
    try:
        _write_route_a_buy_inputs(
            base_dir,
            trading_date="2026-03-22",
            open_price=10.50,
            low_price=10.20,
            high_price=10.80,
            low_limit=9.00,
        )
        result = OpenExecutionManager(base_dir=str(base_dir)).run("2026-03-22")
        decision_df = pd.read_csv(base_dir / "reports" / "daily_open_execution_decision.csv", encoding="utf-8-sig")
        orders_df = pd.read_csv(base_dir / "reports" / "daily_open_execution_orders.csv", encoding="utf-8-sig")

        assert result["order_count"] == 0
        assert bool(decision_df.loc[0, "buy_trigger_hit"]) is False
        assert decision_df.loc[0, "buy_block_reason"] == "TARGET_PRICE_NOT_TOUCHED"
        assert orders_df.empty
    finally:
        shutil.rmtree(base_dir, ignore_errors=True)


def test_route_a_intraday_touch_fills() -> None:
    base_dir = _make_case_dir()
    try:
        _write_route_a_buy_inputs(
            base_dir,
            trading_date="2026-03-22",
            open_price=10.50,
            low_price=9.95,
            high_price=10.80,
            low_limit=9.00,
        )
        result = OpenExecutionManager(base_dir=str(base_dir)).run("2026-03-22")
        decision_df = pd.read_csv(base_dir / "reports" / "daily_open_execution_decision.csv", encoding="utf-8-sig")
        orders_df = pd.read_csv(base_dir / "reports" / "daily_open_execution_orders.csv", encoding="utf-8-sig")

        assert result["order_count"] == 1
        assert bool(decision_df.loc[0, "buy_trigger_hit"]) is True
        assert float(decision_df.loc[0, "planned_buy_price"]) == 10.0
        assert float(decision_df.loc[0, "executed_buy_price"]) == 10.0
        assert float(orders_df.loc[0, "executed_buy_price"]) == 10.0
    finally:
        shutil.rmtree(base_dir, ignore_errors=True)


def test_route_a_limit_down_locked_does_not_buy() -> None:
    base_dir = _make_case_dir()
    try:
        _write_route_a_buy_inputs(
            base_dir,
            trading_date="2026-03-22",
            open_price=9.00,
            low_price=9.00,
            high_price=9.00,
            low_limit=9.00,
        )
        result = OpenExecutionManager(base_dir=str(base_dir)).run("2026-03-22")
        decision_df = pd.read_csv(base_dir / "reports" / "daily_open_execution_decision.csv", encoding="utf-8-sig")

        assert result["order_count"] == 0
        assert decision_df.loc[0, "buy_block_reason"] == "LIMIT_DOWN_LOCKED"
        assert bool(decision_df.loc[0, "buy_trigger_hit"]) is False
    finally:
        shutil.rmtree(base_dir, ignore_errors=True)


def test_route_a_target_profit_reached_sells_next_open() -> None:
    base_dir = _make_case_dir()
    try:
        review_df, next_day_df = _run_close_review_and_next_day(
            base_dir,
            trading_date="2026-03-22",
            close_price=108.50,
            open_price=107.0,
            high_price=109.0,
            low_price=106.5,
            ma20=101.0,
        )
        assert review_df.loc[0, "route_a_exit_signal"] == "TARGET_PROFIT_REACHED"
        assert next_day_df.loc[0, "management_action"] == "次日开盘卖出"
    finally:
        shutil.rmtree(base_dir, ignore_errors=True)


def test_route_a_upper_shadow_warning_sells_next_open() -> None:
    base_dir = _make_case_dir()
    try:
        review_df, next_day_df = _run_close_review_and_next_day(
            base_dir,
            trading_date="2026-03-22",
            close_price=103.00,
            open_price=102.00,
            high_price=110.00,
            low_price=101.00,
            ma20=100.0,
        )
        assert review_df.loc[0, "route_a_exit_signal"] == "UPPER_SHADOW_WARNING"
        assert next_day_df.loc[0, "management_action"] == "次日开盘卖出"
    finally:
        shutil.rmtree(base_dir, ignore_errors=True)


def test_route_a_close_below_ma20_sells_next_open() -> None:
    base_dir = _make_case_dir()
    try:
        review_df, next_day_df = _run_close_review_and_next_day(
            base_dir,
            trading_date="2026-03-22",
            close_price=99.00,
            open_price=100.50,
            high_price=101.00,
            low_price=98.50,
            ma20=100.50,
            avg_fill_price=100.00,
        )
        assert review_df.loc[0, "route_a_exit_signal"] == "CLOSE_BELOW_MA20"
        assert next_day_df.loc[0, "management_action"] == "次日开盘卖出"
    finally:
        shutil.rmtree(base_dir, ignore_errors=True)
