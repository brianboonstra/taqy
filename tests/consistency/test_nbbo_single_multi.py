import datetime
import pandas as pd
from taqy.usequity import taq_nbbo_bars_on_date


def test_consistency_nbbo():
    """
    Ensures single ticker queries agree with multi ticker queries
    """
    fields = [
        "best_bid",
        "best_bidsizeshares",
        "best_ask",
        "best_asksizeshares",
        "time_of_last_quote",
    ]
    tickers = [
        "SPY",
        "JPM",
        "LLY",
        "PBPB",
    ]
    date = datetime.date(2024, 2, 29)
    bar_minutes_ticker_checks = [1, 2, 6, 30, 60]
    for bar_minutes in bar_minutes_ticker_checks:
        bar_all = taq_nbbo_bars_on_date(
            tickers, date=date, bar_minutes=bar_minutes
        ).set_index(["ticker"])
        for ticker in tickers:
            bar_one = taq_nbbo_bars_on_date(
                ticker, date=date, bar_minutes=bar_minutes
            ).reset_index()[fields]
            should_match = bar_all.loc[ticker].reset_index()[fields]
            pd.testing.assert_frame_equal(
                bar_one,
                should_match,
                check_exact=False,
                check_names=False,
                atol=0.01,
            )
