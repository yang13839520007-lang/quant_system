from pathlib import Path
import pandas as pd

REQUIRED_COLS = [
    "date", "code", "open", "high", "low", "close", "preclose",
    "volume", "amount", "pctChg", "turn", "peTTM", "pbMRQ"
]

NUMERIC_COLS = [
    "open", "high", "low", "close", "preclose",
    "volume", "amount", "pctChg", "turn", "peTTM", "pbMRQ"
]


def load_daily_csv(file_path: str | Path) -> pd.DataFrame:
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"找不到文件: {file_path}")

    df = pd.read_csv(file_path, low_memory=False)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"CSV缺失字段: {missing}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()

    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (
        df.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )

    df = df.dropna(subset=["open", "close"]).reset_index(drop=True)
    return df