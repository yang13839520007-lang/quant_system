from __future__ import annotations

import json
from pathlib import Path
import shutil
import uuid

from core.market_risk_guard import MarketRiskGuard


def _make_local_tmp_dir() -> Path:
    path = Path("temp") / f"pytest_stage1_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def test_route_c_allows_when_signal_missing() -> None:
    tmp_path = _make_local_tmp_dir()
    try:
        (tmp_path / "reports").mkdir(parents=True, exist_ok=True)
        guard = MarketRiskGuard(base_dir=tmp_path)

        decision = guard.evaluate_route_c("2026-03-22")

        assert decision.route_enabled is True
        assert decision.route_status == "MARKET_RISK_SIGNAL_MISSING_ALLOWED"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_route_c_blocks_on_explicit_signal() -> None:
    tmp_path = _make_local_tmp_dir()
    try:
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        signal_path = reports_dir / "daily_market_risk_signal.json"
        signal_path.write_text(
            json.dumps({"route_c_enabled": False, "risk_score": 88}, ensure_ascii=False),
            encoding="utf-8",
        )
        guard = MarketRiskGuard(base_dir=tmp_path)

        decision = guard.evaluate_route_c("2026-03-22")

        assert decision.route_enabled is False
        assert decision.route_status == "MARKET_RISK_SIGNAL_BLOCKED"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_route_c_blocks_on_high_risk_score() -> None:
    tmp_path = _make_local_tmp_dir()
    try:
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        signal_path = reports_dir / "daily_market_risk_signal.json"
        signal_path.write_text(
            json.dumps({"route_c_enabled": True, "risk_score": 95}, ensure_ascii=False),
            encoding="utf-8",
        )
        guard = MarketRiskGuard(base_dir=tmp_path)

        decision = guard.evaluate_route_c("2026-03-22")

        assert decision.route_enabled is False
        assert decision.route_status == "MARKET_RISK_SCORE_BLOCKED"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)
