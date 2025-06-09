import datetime
import pandas as pd
from taqy.usequity import taq_trade_bars_on_date


def test_consistency_first_last():
    """
    Ensures the more complicated SQL including first and last trade info yields the same
    base data as the less complicated SQL
    """
    ####################################
    # Simpler SQL agrees with complex:
    tickers = [
        "SPY",
        "JPM",
        "LLY",
        "PBPB",
    ]
    date = datetime.date(2024, 2, 29)

    bar_minutes_ticker_checks = [1, 2, 6, 30, 60]
    colms = [
        "vwap",
        "num_trades",
        "total_qty",
        "mean_price_ignoring_size",
        "median_size",
        "median_price",
        "median_notional",
        "min_price",
        "max_price",
        "min_size",
        "max_size",
    ]

    for exch_restr in [None, ("M", "P")]:
        for bar_minutes in bar_minutes_ticker_checks:
            including_first_last = taq_trade_bars_on_date(
                tickers,
                date=date,
                bar_minutes=bar_minutes,
                group_by_exchange=False,
                restrict_to_exchanges=exch_restr,
                include_first_and_last=True,
            ).set_index(["ticker", "window_time"])
            not_including_first_last = taq_trade_bars_on_date(
                tickers,
                date=date,
                bar_minutes=bar_minutes,
                group_by_exchange=False,
                restrict_to_exchanges=exch_restr,
                include_first_and_last=False,
            ).set_index(
                [
                    "ticker",
                    "window_time",
                ]
            )
            for ticker in tickers:
                bar_incl = including_first_last.loc[ticker]
                bar_not_incl = not_including_first_last.loc[ticker]

                pd.testing.assert_frame_equal(
                    bar_incl[colms],
                    bar_not_incl[colms],
                    check_exact=False,
                    check_names=False,
                    atol=0.01,
                )

    for exch_restr in [None, ("M", "P")]:
        for bar_minutes in bar_minutes_ticker_checks:
            including_first_last = taq_trade_bars_on_date(
                tickers,
                date=date,
                bar_minutes=bar_minutes,
                group_by_exchange=True,
                restrict_to_exchanges=exch_restr,
                include_first_and_last=True,
            ).set_index(
                [
                    "ticker",
                ]
            )
            not_including_first_last = taq_trade_bars_on_date(
                tickers,
                date=date,
                bar_minutes=bar_minutes,
                group_by_exchange=True,
                restrict_to_exchanges=exch_restr,
                include_first_and_last=False,
            ).set_index(
                [
                    "ticker",
                ]
            )
            for ticker in tickers:
                bar_incl = including_first_last.loc[ticker]
                bar_not_incl = not_including_first_last.loc[ticker]

                by_exch_incl = bar_incl.set_index("ex")
                by_exch_not_incl = bar_not_incl.set_index("ex")
                for exch in bar_not_incl["ex"].unique():
                    exch_incl = by_exch_incl.loc[exch]
                    exch_not_incl = by_exch_not_incl.loc[exch]
                    pd.testing.assert_frame_equal(
                        exch_incl[colms],
                        exch_not_incl[colms],
                        check_exact=False,
                        check_names=False,
                        atol=0.01,
                    )
