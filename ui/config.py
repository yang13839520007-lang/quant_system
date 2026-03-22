from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import sys

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 fallback
    import tomli as tomllib

from ui.display_labels import get_message, get_page_label


@dataclass(frozen=True)
class ReportSpec:
    """UI page definition for a report-backed table view."""

    key: str
    title: str
    csv_name: str
    summary_name: str | None = None
    empty_message: str = get_message("file_missing", "文件不存在/尚未生成")


DEFAULT_REPORT_SPECS = (
    ReportSpec(
        key="candidates",
        title=get_page_label("candidates"),
        csv_name="daily_candidates_top20.csv",
        summary_name="daily_candidates_summary.txt",
    ),
    ReportSpec(
        key="trade_plan",
        title=get_page_label("trade_plan"),
        csv_name="daily_trade_plan_top10.csv",
        summary_name="daily_execution_plan_summary.txt",
    ),
    ReportSpec(
        key="portfolio",
        title=get_page_label("portfolio"),
        csv_name="daily_portfolio_plan_risk_checked.csv",
        summary_name="daily_portfolio_summary_risk_checked.txt",
    ),
    ReportSpec(
        key="open_execution",
        title=get_page_label("open_execution"),
        csv_name="daily_open_execution_orders.csv",
        summary_name="daily_open_execution_summary.txt",
    ),
    ReportSpec(
        key="intraday_recheck",
        title=get_page_label("intraday_recheck"),
        csv_name="daily_intraday_recheck_orders.csv",
        summary_name="daily_intraday_recheck_summary.txt",
    ),
    ReportSpec(
        key="close_review",
        title=get_page_label("close_review"),
        csv_name="daily_close_review.csv",
        summary_name="daily_close_review_summary.txt",
    ),
    ReportSpec(
        key="next_day_management",
        title=get_page_label("next_day_management"),
        csv_name="daily_next_day_management.csv",
        summary_name="daily_next_day_management_summary.txt",
    ),
)


@dataclass(frozen=True)
class AppConfig:
    """Centralized UI configuration."""

    project_root: Path
    reports_dir: Path
    logs_dir: Path
    orchestrator_entry: Path
    python_executable: Path
    auto_refresh_ms: int
    window_title: str
    window_width: int
    window_height: int
    runtime_log_limit: int
    report_specs: tuple[ReportSpec, ...] = DEFAULT_REPORT_SPECS


DEFAULT_CONFIG_FILE = Path(__file__).with_name("ui_config.toml")


def load_config(config_path: str | Path | None = None) -> AppConfig:
    """Load UI configuration from TOML with safe defaults."""

    selected_path = Path(config_path or os.getenv("QUANT_UI_CONFIG") or DEFAULT_CONFIG_FILE)
    config_dir = selected_path.parent
    raw = _read_toml_file(selected_path)

    project_root = _resolve_path(
        config_dir,
        raw.get("project_root"),
        fallback=Path(__file__).resolve().parent.parent,
    )
    reports_dir = _resolve_path(project_root, raw.get("reports_dir"), fallback=project_root / "reports")
    logs_dir = _resolve_path(project_root, raw.get("logs_dir"), fallback=project_root / "temp" / "ui_logs")
    orchestrator_entry = _resolve_path(
        project_root,
        raw.get("orchestrator_entry"),
        fallback=project_root / "ui" / "orchestrator_entry.py",
    )

    python_executable_raw = str(raw.get("python_executable", "")).strip()
    python_executable = Path(python_executable_raw) if python_executable_raw else Path(sys.executable)

    return AppConfig(
        project_root=project_root,
        reports_dir=reports_dir,
        logs_dir=logs_dir,
        orchestrator_entry=orchestrator_entry,
        python_executable=python_executable,
        auto_refresh_ms=max(5_000, int(raw.get("auto_refresh_ms", 30_000))),
        window_title=str(raw.get("window_title", "A股量化交易监控终端")),
        window_width=max(1200, int(raw.get("window_width", 1600))),
        window_height=max(800, int(raw.get("window_height", 980))),
        runtime_log_limit=max(200, int(raw.get("runtime_log_limit", 800))),
    )


def _read_toml_file(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    return data if isinstance(data, dict) else {}


def _resolve_path(base_dir: Path, configured_value: object, fallback: Path) -> Path:
    if configured_value is None:
        return fallback.resolve()

    value = str(configured_value).strip()
    if not value:
        return fallback.resolve()

    candidate = Path(value)
    if candidate.is_absolute():
        return candidate.resolve()
    return (base_dir / candidate).resolve()
