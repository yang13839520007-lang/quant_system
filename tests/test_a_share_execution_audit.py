from __future__ import annotations

import pandas as pd

from core.a_share_execution_audit import AShareExecutionAuditor


def _base_snapshot() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "sz.000001",
                "name": "平安银行",
                "paused": False,
                "open_price": 10.00,
                "high_price": 10.20,
                "low_price": 9.90,
                "high_limit": 11.00,
                "low_limit": 9.00,
                "snapshot_quality": "EXECUTION_READY",
                "source_trade_date": "2026-03-22",
            }
        ]
    )


def _base_orders(action: str = "BUY") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trading_date": "2026-03-22",
                "code": "sz.000001",
                "name": "平安银行",
                "action": action,
                "order_shares": 1000,
                "order_price": 10.00,
                "order_type": "LIMIT",
                "status": "PENDING",
            }
        ]
    )


def test_blocks_st_buy() -> None:
    auditor = AShareExecutionAuditor()
    snapshot = _base_snapshot()
    snapshot.loc[0, "name"] = "*ST测试"

    audit_df, _ = auditor.audit_orders("2026-03-22", _base_orders("BUY"), snapshot)

    assert audit_df.iloc[0]["audit_status"] == "BLOCKED"
    assert audit_df.iloc[0]["violation_codes"] == "ST_SECURITY_BUY_BLOCKED"


def test_blocks_paused_security() -> None:
    auditor = AShareExecutionAuditor()
    snapshot = _base_snapshot()
    snapshot.loc[0, "paused"] = True

    audit_df, _ = auditor.audit_orders("2026-03-22", _base_orders("BUY"), snapshot)

    assert audit_df.iloc[0]["audit_status"] == "BLOCKED"
    assert audit_df.iloc[0]["violation_codes"] == "TRADE_SUSPENDED"


def test_blocks_t1_locked_sell() -> None:
    auditor = AShareExecutionAuditor()
    positions = pd.DataFrame([{"code": "sz.000001", "available_qty": 0}])

    audit_df, _ = auditor.audit_orders("2026-03-22", _base_orders("SELL"), _base_snapshot(), positions)

    assert audit_df.iloc[0]["audit_status"] == "BLOCKED"
    assert audit_df.iloc[0]["violation_codes"] == "T1_SELL_LOCKED"


def test_blocks_one_word_limit_up_buy() -> None:
    auditor = AShareExecutionAuditor()
    snapshot = _base_snapshot()
    snapshot.loc[0, "open_price"] = 11.00
    snapshot.loc[0, "low_price"] = 11.00
    snapshot.loc[0, "high_limit"] = 11.00

    audit_df, _ = auditor.audit_orders("2026-03-22", _base_orders("BUY"), snapshot)

    assert audit_df.iloc[0]["audit_status"] == "BLOCKED"
    assert audit_df.iloc[0]["violation_codes"] == "ONE_WORD_LIMIT_UP_BUY_BLOCKED"


def test_blocks_one_word_limit_down_sell() -> None:
    auditor = AShareExecutionAuditor()
    snapshot = _base_snapshot()
    snapshot.loc[0, "open_price"] = 9.00
    snapshot.loc[0, "high_price"] = 9.00
    snapshot.loc[0, "low_limit"] = 9.00
    positions = pd.DataFrame([{"code": "sz.000001", "available_qty": 1000}])

    audit_df, _ = auditor.audit_orders("2026-03-22", _base_orders("SELL"), snapshot, positions)

    assert audit_df.iloc[0]["audit_status"] == "BLOCKED"
    assert audit_df.iloc[0]["violation_codes"] == "ONE_WORD_LIMIT_DOWN_SELL_BLOCKED"


def test_blocks_adjusted_or_stale_price_data() -> None:
    auditor = AShareExecutionAuditor()
    snapshot = _base_snapshot()
    snapshot.loc[0, "snapshot_quality"] = "REPLAY_PROXY"
    snapshot.loc[0, "source_trade_date"] = "2026-03-21"

    audit_df, _ = auditor.audit_orders("2026-03-22", _base_orders("BUY"), snapshot)

    assert audit_df.iloc[0]["audit_status"] == "BLOCKED"
    assert audit_df.iloc[0]["violation_codes"] == "ADJUSTED_OR_STALE_PRICE_DATA"


def test_estimates_slippage_and_fee_for_clean_buy() -> None:
    auditor = AShareExecutionAuditor()

    audit_df, summary = auditor.audit_orders("2026-03-22", _base_orders("BUY"), _base_snapshot())
    row = audit_df.iloc[0]

    assert row["audit_status"] == "PASS"
    assert row["audited_fill_price"] == 10.008
    assert row["commission"] == 5.0
    assert row["total_fee"] == 5.0
    assert row["slippage_amount"] == 8.0
    assert summary["passed_orders"] == 1
