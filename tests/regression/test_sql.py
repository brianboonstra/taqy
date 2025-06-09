import os
import datetime
import pytest


from taqy.usequity import taq_trade_bar_statistics_sql

GOLD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "gold_files")


def read_gold_sql(filename):
    filepath = os.path.join(GOLD_DIR, filename)
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Expected gold file not found: {filepath}")
    with open(filepath, "r") as f:
        return f.read()


def write_gold_sql(filename, content):
    filepath = os.path.join(GOLD_DIR, filename)
    with open(filepath, "w") as f:
        f.write(content)


@pytest.mark.parametrize(
    "tickers, date, bar_minutes, group_by_exchange, restrict_to_exchanges,  gold_filename",
    [
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            False,
            None,
            "taq_trade_bar_statistics_sql_6_F_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            False,
            None,
            "taq_trade_bar_statistics_sql_60_F_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            True,
            None,
            "taq_trade_bar_statistics_sql_6_T_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            True,
            None,
            "taq_trade_bar_statistics_sql_60_T_F.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            False,
            ("D", "P"),
            "taq_trade_bar_statistics_sql_6_F_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            False,
            ("D", "P"),
            "taq_trade_bar_statistics_sql_60_F_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            6,
            True,
            ("D", "P"),
            "taq_trade_bar_statistics_sql_6_T_T.sql",
        ),
        (
            ["SPY", "JPM", "LLY"],
            datetime.date(2024, 2, 29),
            60,
            True,
            ("D", "P"),
            "taq_trade_bar_statistics_sql_60_T_T.sql",
        ),
    ],
)
def test_taq_trade_bar_statistics_sql(
    tickers,
    date,
    bar_minutes,
    group_by_exchange,
    restrict_to_exchanges,
    gold_filename,
    request,
):
    stat_sql = taq_trade_bar_statistics_sql(
        tickers,
        date,
        bar_minutes=bar_minutes,
        group_by_exchange=group_by_exchange,
        restrict_to_exchanges=restrict_to_exchanges,
    )

    if request.config.getoption("--update-gold"):
        write_gold_sql(gold_filename, stat_sql)
        pytest.skip("Updated gold file")
    else:
        expected = read_gold_sql(gold_filename)
        assert stat_sql == expected
