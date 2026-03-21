from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch the existing orchestrator from the UI layer.")
    parser.add_argument("--project-root", required=True, help="Project root directory.")
    parser.add_argument("--trading-date", required=True, help="Trading date in YYYY-MM-DD.")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on the first failed stage.")
    parser.add_argument(
        "--strict-realtime-core",
        action="store_true",
        help="Enable strict realtime mode for core stages.",
    )
    parser.add_argument(
        "--reuse-violation-action",
        default="warn",
        choices=("warn", "reject"),
        help="Reuse policy action passed to the orchestrator manager.",
    )
    parser.add_argument(
        "--enable-replay-validation",
        action="store_true",
        help="Enable replay validation stage from the UI launcher.",
    )
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from trading_day_orchestrator_manager import TradingDayOrchestratorManager

    manager = TradingDayOrchestratorManager(
        base_dir=str(project_root),
        enable_replay_validation=args.enable_replay_validation,
        fail_fast=args.fail_fast,
        strict_realtime_core=args.strict_realtime_core,
        reuse_violation_action=args.reuse_violation_action,
    )
    summary = manager.run(trading_date=args.trading_date)
    overall_status = str(summary.get("overall_status", "")).upper()
    return 0 if overall_status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
