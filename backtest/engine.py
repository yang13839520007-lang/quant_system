import pandas as pd


def run_single_stock_backtest(
    df: pd.DataFrame,
    initial_cash: float = 100000.0,
    commission_rate: float = 0.0003,
    stamp_tax_rate: float = 0.001,
    lot_size: int = 100,
):
    cash = initial_cash
    position = 0
    entry_date = None
    entry_total_cost = 0.0

    trades = []
    equity_curve = []

    if len(df) < 20:
        raise ValueError("数据太少，至少需要20行以上")

    for i in range(1, len(df)):
        prev_row = df.iloc[i - 1]
        row = df.iloc[i]

        trade_date = row["date"]
        open_price = row["open"]
        close_price = row["close"]

        if pd.isna(open_price) or open_price <= 0:
            market_value = position * close_price if position > 0 else 0.0
            equity_curve.append({
                "date": trade_date,
                "cash": cash,
                "position": position,
                "market_value": market_value,
                "equity": cash + market_value
            })
            continue

        can_sell = position > 0 and entry_date is not None and trade_date > entry_date

        if can_sell and bool(prev_row["sell_signal"]):
            gross_amount = position * open_price
            sell_commission = max(gross_amount * commission_rate, 5.0)
            stamp_tax = gross_amount * stamp_tax_rate
            net_amount = gross_amount - sell_commission - stamp_tax

            cash += net_amount
            pnl = net_amount - entry_total_cost
            return_pct = pnl / entry_total_cost * 100 if entry_total_cost > 0 else 0.0

            trades.append({
                "date": trade_date,
                "action": "SELL",
                "price": round(open_price, 4),
                "shares": position,
                "gross_amount": round(gross_amount, 2),
                "commission": round(sell_commission, 2),
                "stamp_tax": round(stamp_tax, 2),
                "net_amount": round(net_amount, 2),
                "pnl": round(pnl, 2),
                "return_pct": round(return_pct, 2),
                "cash_after": round(cash, 2),
            })

            position = 0
            entry_date = None
            entry_total_cost = 0.0

        if position == 0 and bool(prev_row["buy_signal"]):
            raw_shares = int(cash // (open_price * (1 + commission_rate)))
            buy_shares = (raw_shares // lot_size) * lot_size

            if buy_shares >= lot_size:
                gross_amount = buy_shares * open_price
                buy_commission = max(gross_amount * commission_rate, 5.0)
                total_cost = gross_amount + buy_commission

                if total_cost <= cash:
                    cash -= total_cost
                    position = buy_shares
                    entry_date = trade_date
                    entry_total_cost = total_cost

                    trades.append({
                        "date": trade_date,
                        "action": "BUY",
                        "price": round(open_price, 4),
                        "shares": position,
                        "gross_amount": round(gross_amount, 2),
                        "commission": round(buy_commission, 2),
                        "stamp_tax": 0.0,
                        "net_amount": round(-total_cost, 2),
                        "pnl": None,
                        "return_pct": None,
                        "cash_after": round(cash, 2),
                    })

        market_value = position * close_price if position > 0 else 0.0
        equity = cash + market_value

        equity_curve.append({
            "date": trade_date,
            "cash": round(cash, 2),
            "position": position,
            "market_value": round(market_value, 2),
            "equity": round(equity, 2),
        })

    equity_df = pd.DataFrame(equity_curve)
    trade_df = pd.DataFrame(trades)

    final_equity = equity_df.iloc[-1]["equity"] if not equity_df.empty else initial_cash
    total_return = (final_equity / initial_cash - 1) * 100

    sell_df = trade_df[trade_df["action"] == "SELL"].copy() if not trade_df.empty else pd.DataFrame()

    trade_count = len(sell_df)
    win_rate = (sell_df["pnl"] > 0).mean() * 100 if trade_count > 0 else 0.0
    avg_pnl = sell_df["pnl"].mean() if trade_count > 0 else 0.0
    avg_return = sell_df["return_pct"].mean() if trade_count > 0 else 0.0

    summary = {
        "initial_cash": round(initial_cash, 2),
        "final_equity": round(final_equity, 2),
        "total_return_pct": round(total_return, 2),
        "trade_count": int(trade_count),
        "win_rate_pct": round(win_rate, 2),
        "avg_pnl": round(avg_pnl, 2),
        "avg_return_pct": round(avg_return, 2),
    }

    return summary, trade_df, equity_df