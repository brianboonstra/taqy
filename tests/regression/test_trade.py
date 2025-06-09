import os
import pytest
import datetime

import pandas as pd

from taqy.usequity import taq_trade_bars_on_date

GOLD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "gold_files")


def read_gold_parquet(filename):
    filepath = os.path.join(GOLD_DIR, filename)
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Expected gold parquet file not found: {filepath}")
    df = pd.read_parquet(os.path.join(GOLD_DIR, filepath))
    return df


def write_gold_parquet(filename, df):
    filepath = os.path.join(GOLD_DIR, filename)
    df.to_parquet(os.path.join(GOLD_DIR, filepath))


@pytest.mark.parametrize(
    "tickers, date, bar_minutes, group_by_exchange, restrict_to_exchanges, include_first_and_last, gold_filename",
    [
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            False,
            None,
            False,
            "taq_trade_bar_statistics_sql_6_F_F_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            False,
            None,
            False,
            "taq_trade_bar_statistics_sql_60_F_F_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            True,
            None,
            False,
            "taq_trade_bar_statistics_sql_6_T_F_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            True,
            None,
            False,
            "taq_trade_bar_statistics_sql_60_T_F_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            False,
            ("D", "P"),
            False,
            "taq_trade_bar_statistics_sql_6_F_T_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            False,
            ("D", "P"),
            False,
            "taq_trade_bar_statistics_sql_60_F_T_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            True,
            ("D", "P"),
            False,
            "taq_trade_bar_statistics_sql_6_T_T_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            True,
            ("D", "P"),
            False,
            "taq_trade_bar_statistics_sql_60_T_T_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            False,
            None,
            True,
            "taq_trade_bar_statistics_sql_6_F_F_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            False,
            None,
            True,
            "taq_trade_bar_statistics_sql_60_F_F_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            True,
            None,
            True,
            "taq_trade_bar_statistics_sql_6_T_F_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            True,
            None,
            True,
            "taq_trade_bar_statistics_sql_60_T_F_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            False,
            ("D", "P"),
            True,
            "taq_trade_bar_statistics_sql_6_F_T_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            False,
            ("D", "P"),
            True,
            "taq_trade_bar_statistics_sql_60_F_T_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            True,
            ("D", "P"),
            True,
            "taq_trade_bar_statistics_sql_6_T_T_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            True,
            ("D", "P"),
            True,
            "taq_trade_bar_statistics_sql_60_T_T_T.sql",
        ),
    ],
)
def test_taq_trade_bar_statistics_sql(
    tickers,
    date,
    bar_minutes,
    group_by_exchange,
    restrict_to_exchanges,
    include_first_and_last,
    gold_filename,
    request,
):
    bars = taq_trade_bars_on_date(
        tickers,
        date=date,
        bar_minutes=bar_minutes,
        group_by_exchange=group_by_exchange,
        restrict_to_exchanges=restrict_to_exchanges,
        include_first_and_last=include_first_and_last,
    )
    gold_loc = os.path.join(GOLD_DIR, gold_filename)

    numeric_fields = [
        "num_trades",
        "total_qty",
        "vwap",
        "max_price",
        "min_price",
        "max_size",
        "min_size",
        "median_size",
        "median_notional",
    ]

    if request.config.getoption("--update-gold"):
        write_gold_parquet(gold_loc, bars)
        pytest.skip("Updated gold file")
    else:
        expected = read_gold_parquet(gold_loc)
        pd.testing.assert_frame_equal(
            bars[numeric_fields],
            expected[numeric_fields],
            check_exact=False,
            check_names=False,
            atol=0.01,
        )
        if "ex" in expected.columns:
            pd.testing.assert_series_equal(bars["ex"], expected["ex"])
        pd.testing.assert_series_equal(bars["window_time"], expected["window_time"])
