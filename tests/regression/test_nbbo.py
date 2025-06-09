import os
import pytest
import datetime

import pandas as pd

from taqy.usequity import taq_nbbo_bars_on_date

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
    "tickers, date, bar_minutes, gold_filename",
    [
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            "nbbo_big_6_Feb.parquet",
        ),
        (
            ["SPY", "PBPB", "HLIT"],
            datetime.date(2024, 2, 29),
            6,
            "nbbo_small_6_Feb.parquet",
        ),
        (
            "LLY",
            datetime.date(2024, 2, 29),
            6,
            "nbbo_single_6_Feb.parquet",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 7, 25),
            6,
            "nbbo_big_60_July.parquet",
        ),
        (
            ["SPY", "PBPB", "HLIT"],
            datetime.date(2024, 7, 25),
            6,
            "nbbo_small_60_July.parquet",
        ),
    ],
)
def test_taq_nbbo_bars_on_date(
    tickers,
    date,
    bar_minutes,
    gold_filename,
    request,
):

    bars = taq_nbbo_bars_on_date(tickers, date=date, bar_minutes=bar_minutes)
    gold_loc = os.path.join(GOLD_DIR, gold_filename)

    numeric_fields = [
        "best_bid",
        "best_bidsizeshares",
        "best_ask",
        "best_asksizeshares",
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
        pd.testing.assert_series_equal(bars["window_time"], expected["window_time"])
