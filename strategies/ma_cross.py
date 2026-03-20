import pandas as pd


def generate_ma_cross_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["ma_short_prev"] = df["ma_short"].shift(1)
    df["ma_long_prev"] = df["ma_long"].shift(1)

    df["buy_signal"] = (
        (df["ma_short_prev"] <= df["ma_long_prev"]) &
        (df["ma_short"] > df["ma_long"])
    )

    df["sell_signal"] = (
        (df["ma_short_prev"] >= df["ma_long_prev"]) &
        (df["ma_short"] < df["ma_long"])
    )

    return df