import pandas as pd


def add_moving_averages(df: pd.DataFrame, short_window: int = 5, long_window: int = 10) -> pd.DataFrame:
    df = df.copy()
    df["ma_short"] = df["close"].rolling(short_window).mean()
    df["ma_long"] = df["close"].rolling(long_window).mean()
    return df