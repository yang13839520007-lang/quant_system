from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any

import pandas as pd


def _load_optional_settings():
    try:
        from config import settings  # type: ignore
        return settings
    except Exception:
        return None


def _get_setting(name: str, default: Any) -> Any:
    settings = _load_optional_settings()
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    return default


@dataclass(frozen=True)
class MarketRiskConfig:
    route_name: str = "ROUTE_C"
    route_c_enabled: bool = True
    block_on_missing_signal: bool = False
    signal_score_threshold: float = 70.0
    signal_file_candidates: tuple[str, ...] = (
        "daily_market_risk_signal.json",
        "market_risk_signal.json",
    )


@dataclass(frozen=True)
class MarketRiskDecision:
    trading_date: str
    route_name: str
    route_enabled: bool
    route_status: str
    risk_score: float | None
    source_path: str
    reason: str
    source_mode: str
    signal_present: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MarketRiskGuard:
    """Route C outer switch for market-level execution gating."""

    def __init__(self, base_dir: str | Path, config: MarketRiskConfig | None = None) -> None:
        self.base_dir = Path(base_dir)
        self.reports_dir = self.base_dir / "reports"
        self.config = config or self._build_default_config()

    def evaluate_route_c(self, trading_date: str, snapshot_df: pd.DataFrame | None = None) -> MarketRiskDecision:
        del snapshot_df  # Stage 1: explicit signal first, no heuristic market override yet.

        if not self.config.route_c_enabled:
            return MarketRiskDecision(
                trading_date=trading_date,
                route_name=self.config.route_name,
                route_enabled=True,
                route_status="ROUTE_C_SWITCH_DISABLED",
                risk_score=None,
                source_path="",
                reason="Route C 大盘风控开关已关闭，执行层不做大盘拦截。",
                source_mode="config",
                signal_present=False,
            )

        payload, signal_path = self._load_signal_payload()
        if not payload:
            if self.config.block_on_missing_signal:
                return MarketRiskDecision(
                    trading_date=trading_date,
                    route_name=self.config.route_name,
                    route_enabled=False,
                    route_status="MARKET_RISK_SIGNAL_MISSING_BLOCKED",
                    risk_score=None,
                    source_path="",
                    reason="未找到大盘风控信号文件，Route C 外层开关按阻断处理。",
                    source_mode="missing_signal",
                    signal_present=False,
                )
            return MarketRiskDecision(
                trading_date=trading_date,
                route_name=self.config.route_name,
                route_enabled=True,
                route_status="MARKET_RISK_SIGNAL_MISSING_ALLOWED",
                risk_score=None,
                source_path="",
                reason="未找到大盘风控信号文件，Route C 外层开关默认放行。",
                source_mode="missing_signal",
                signal_present=False,
            )

        explicit_allowed = self._extract_allowed_flag(payload)
        risk_score = self._extract_risk_score(payload)
        if explicit_allowed is False:
            return MarketRiskDecision(
                trading_date=trading_date,
                route_name=self.config.route_name,
                route_enabled=False,
                route_status="MARKET_RISK_SIGNAL_BLOCKED",
                risk_score=risk_score,
                source_path=str(signal_path),
                reason="大盘风控信号明确关闭 Route C。",
                source_mode="signal",
                signal_present=True,
            )
        if risk_score is not None and risk_score >= float(self.config.signal_score_threshold):
            return MarketRiskDecision(
                trading_date=trading_date,
                route_name=self.config.route_name,
                route_enabled=False,
                route_status="MARKET_RISK_SCORE_BLOCKED",
                risk_score=risk_score,
                source_path=str(signal_path),
                reason=f"大盘风控风险分 {risk_score:.2f} 超过阈值 {self.config.signal_score_threshold:.2f}，阻断 Route C。",
                source_mode="signal",
                signal_present=True,
            )
        return MarketRiskDecision(
            trading_date=trading_date,
            route_name=self.config.route_name,
            route_enabled=True,
            route_status="MARKET_RISK_ALLOWED",
            risk_score=risk_score,
            source_path=str(signal_path),
            reason="大盘风控信号允许 Route C 执行。",
            source_mode="signal",
            signal_present=True,
        )

    def _load_signal_payload(self) -> tuple[dict[str, Any], Path | None]:
        for filename in self.config.signal_file_candidates:
            path = self.reports_dir / filename
            if not path.exists():
                continue
            try:
                with path.open("r", encoding="utf-8") as fh:
                    payload = json.load(fh)
            except Exception:
                continue
            if isinstance(payload, dict):
                return payload, path
        return {}, None

    def _extract_allowed_flag(self, payload: dict[str, Any]) -> bool | None:
        for key in ("route_c_enabled", "route_c_allowed", "allow_route_c", "allow_trading"):
            if key not in payload:
                continue
            value = payload.get(key)
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in {"true", "1", "yes", "y", "allow", "allowed", "on"}:
                return True
            if text in {"false", "0", "no", "n", "block", "blocked", "off"}:
                return False
        return None

    def _extract_risk_score(self, payload: dict[str, Any]) -> float | None:
        for key in ("risk_score", "market_risk_score", "score"):
            if key not in payload:
                continue
            try:
                return float(payload.get(key))
            except (TypeError, ValueError):
                return None
        return None

    def _build_default_config(self) -> MarketRiskConfig:
        return MarketRiskConfig(
            route_c_enabled=bool(_get_setting("MARKET_RISK_ROUTE_C_ENABLED", True)),
            block_on_missing_signal=bool(_get_setting("MARKET_RISK_BLOCK_ON_MISSING_SIGNAL", False)),
            signal_score_threshold=float(_get_setting("MARKET_RISK_SIGNAL_SCORE_THRESHOLD", 70.0)),
        )
