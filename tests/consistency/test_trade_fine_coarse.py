import datetime
import pandas as pd
import numpy as np
from taqy.usequity import taq_trade_bars_on_date


def consistency_check_bar_sums():
    """
    Ensures bars of length N * dt have sums equaling the ones found in N bars of length dt
    """
    tickers = [
        "SPY",
        "JPM",
        "LLY",
    ]
    exchanges_to_actually_check = ("D", "P", "Z")
    date = datetime.date(2024, 2, 29)

    def group_up(df: pd.DataFrame, n: int, offset: int = 0):
        times = df.reset_index()["window_time"]
        gup = df[["num_trades", "total_qty", "vwap"]].copy()
        gup["notional"] = gup["total_qty"] * gup["vwap"]
        if n > 1:
            gixs = (offset + np.arange(df.shape[0])) // n
            gup = gup.groupby(gixs).sum()
            ix = np.arange(len(times))[offset::n]
            if offset:
                wtime_ix = np.arange(len(times))[::n]
            else:
                wtime_ix = np.arange(len(times))[(n - 1) :: n]
            times = times.iloc[wtime_ix]
        gup = gup.reset_index()[["num_trades", "total_qty", "notional"]].set_index(
            times
        )
        return gup

    for group_by_exchange in (False, True):
        for exch_restr in [None, ("D", "P")]:
            for bar_minutes, prev_bar_minutes in ((60, 30), (30, 15), (15, 5)):
                prev_bar_raw = taq_trade_bars_on_date(
                    tickers,
                    date=date,
                    bar_minutes=prev_bar_minutes,
                    group_by_exchange=group_by_exchange,
                    restrict_to_exchanges=exch_restr,
                    include_first_and_last=False,
                )
                this_bar = taq_trade_bars_on_date(
                    tickers,
                    date=date,
                    bar_minutes=bar_minutes,
                    group_by_exchange=group_by_exchange,
                    restrict_to_exchanges=exch_restr,
                    include_first_and_last=False,
                )
                ratio = bar_minutes / prev_bar_minutes
                assert ratio == int(ratio)
                if group_by_exchange:
                    grouping = ["ticker", "ex"]
                    this_bar = this_bar.loc[
                        this_bar.ex.isin(exchanges_to_actually_check)
                    ].copy()
                    prev_bar = prev_bar_raw.loc[
                        prev_bar_raw.ex.isin(exchanges_to_actually_check)
                    ].copy()
                else:
                    grouping = ["ticker"]
                    this_bar = this_bar.copy()
                    prev_bar = prev_bar_raw.copy()
                offset = 1 if (bar_minutes == 60) else 0
                try:
                    this_grp = this_bar.groupby(grouping).apply(
                        group_up, n=1, offset=0, include_groups=False
                    )
                    prev_grp = prev_bar.groupby(grouping).apply(
                        group_up, n=int(ratio), offset=offset, include_groups=False
                    )
                except Exception:
                    print(this_grp)
                    print(
                        f"Error for bars at {bar_minutes} versus prev {prev_bar_minutes}, exch_restr {exch_restr}, group_by_exchange {group_by_exchange}"
                    )
                    raise

                pd.testing.assert_frame_equal(
                    this_grp,
                    prev_grp,
                    check_exact=False,
                    check_names=False,
                    atol=0.01,
                )
