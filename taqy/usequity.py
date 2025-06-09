import datetime
import pytz

import pandas as pd
import wrds

from .utils import HidePrinting, _make_timestamp

# License: GPLv3 or later
# Copyright 2025 by Brian K. Boonstra


"""
Wharton Research Data Services (WRDS) has subsecond-level trade, bid and offer data based on
the NYSE TAQ. However, downloading the full data series is not practical due to the hundreds
of millions of rows per day. Here, we use SQL to generate more manageable "bars" on the 
server side.

Information about the source data set is mainly available from NYSE at:
https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf
"""

DEFAULT_WRDS_CONNECTION: wrds.sql.Connection | None = None
CACHED_QUERIES: dict[str, pd.DataFrame] = {}

TIME_COLUMNS = ("time_m", "time_of_last_quote", "last_trade_time", "first_trade_time")
DATE_COLUMNS = (
    "date",
    "window_time",
)

##########################
# Connection management ##
##########################


def set_default_connection(cnxn: wrds.sql.Connection):
    global DEFAULT_WRDS_CONNECTION
    print(f"Setting default WRDS connection to a {cnxn.__class__}")
    DEFAULT_WRDS_CONNECTION = cnxn


def get_wrds_connection(
    wrds_db: wrds.sql.Connection | None = None,
) -> wrds.sql.Connection:
    db = wrds_db or DEFAULT_WRDS_CONNECTION
    if db is None:
        raise ValueError(
            "Please initialize the module-level DB with connect_to_wrds(...), or specify a wrds Connection object to use as wrds_db"
        )
    elif db.connection.closed:
        raise ValueError("The database connection has been closed.  Please reconnect.")
    return db


def connect_to_wrds(reconnect: bool = False, **kwargs):
    if DEFAULT_WRDS_CONNECTION is None or DEFAULT_WRDS_CONNECTION.connection.closed:
        reconnect = True
    if reconnect:
        set_default_connection(wrds.Connection(**kwargs))


def cached_sql(
    db: wrds.sql.Connection,
    sql: str,
    time_cols: tuple[str] = TIME_COLUMNS,
    date_cols: tuple[str] = DATE_COLUMNS,
):
    """
    Query WRDS with reasonable parameters for our purposes

    TODO: If sqlalchemy ever works nicely with decimal types, start using those
    """
    # Standard lru_cache decorator will not play nice with the db arg.  No great
    # workaround at this time
    if sql in CACHED_QUERIES:
        return CACHED_QUERIES[sql].copy()

    df = db.raw_sql(
        sql,
        coerce_float=True,  # This is the default but let's remember it's being done
        date_cols=list(date_cols),
    )

    # WRDS itself has no support for time-of-day columns
    for time_col in time_cols:
        if time_col in df.columns:
            df[time_col] = pd.to_datetime(df[time_col], format="%H:%M:%S.%f").dt.time

    CACHED_QUERIES[sql] = df.copy()

    return df


#################################
## Construction of SQL Queries ##
#################################


def bar_sql(bar_minutes: int) -> str:
    assert bar_minutes == 60 or (bar_minutes <= 30 and 30 % bar_minutes == 0)
    return f"sym_root, date, EXTRACT(HOUR FROM time_m), DIV(EXTRACT(MINUTE FROM time_m),{bar_minutes})"


def window_time_sql(bar_minutes: int) -> str:
    assert bar_minutes == 60 or (bar_minutes <= 30 and 30 % bar_minutes == 0)
    if bar_minutes == 60:
        return "date + (1 + EXTRACT(HOUR FROM time_m) || ':00')::interval"
    else:
        return f"date + (EXTRACT(HOUR FROM time_m) || ':' || {bar_minutes} * DIV(EXTRACT(MINUTE FROM time_m),{bar_minutes}))::interval + ( '00:{bar_minutes}' )::interval"


def taq_trade_bar_select_sql(
    tickers: list[str] | str, restrict_to_exchanges: tuple[str, ...] | str | None = None
) -> str:
    assert bool(tickers)

    if hasattr(tickers, "strip"):  # Allow single ticker as argument
        symbol_select = f"sym_root = '{tickers}'"
    elif len(tickers) == 1:
        symbol_select = f"sym_root = '{tickers[0]}'"
    else:
        symbol_select = f"sym_root IN {tuple(tickers)!r}"

    if restrict_to_exchanges is None:
        exchange_select = "1=1"
    else:
        if hasattr(restrict_to_exchanges, "strip"):  # Allow single exchange as argument
            exchange_select = f"ex = {restrict_to_exchanges}"
        elif len(restrict_to_exchanges) == 1:
            exchange_select = f"ex = {restrict_to_exchanges[0]}"
        else:
            exchange_select = f"ex IN {tuple(restrict_to_exchanges)}"

    ssql = f"""{exchange_select}
                  AND {symbol_select}
                  AND sym_suffix IS NULL
                  AND time_m > '09:30:00' AND time_m < '16:00:00'"""

    return ssql


def taq_trade_bar_statistics_sql(
    tickers: list[str] | str,
    date: datetime.date,
    bar_minutes: int = 30,
    group_by_exchange: bool = False,
    restrict_to_exchanges: tuple[str, ...] | str | None = None,
) -> str:
    """
    Return SQL suitable for finding aggregate bar statistics from WRDS / TAQ
    """
    date_str = date.strftime("%Y%m%d")
    year_str = date.strftime("%Y")
    db_name = f"taqm_{year_str}"
    table_name = f"ctm_{date_str} "

    fields = f"""sym_root AS ticker
                    , date
                    , {window_time_sql(bar_minutes)} AS window_time
                    , COUNT(size) AS num_trades
                    , SUM(size) AS total_qty
                    , SUM(price * size) / SUM(size) AS vwap
                    , AVG(price) AS mean_price_ignoring_size
                    , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY size) AS median_size
                    , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price
                    , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price*size) AS median_notional
                    , MAX(price) AS max_price
                    , MIN(price) AS min_price
                    , MAX(size) AS max_size
                    , MIN(size) AS min_size"""

    grouping = bar_sql(bar_minutes)

    if group_by_exchange:
        grouping += ", ex"
        fields += "\n                    , ex"

    tbsql = f"""SELECT
                    {fields}
                FROM {db_name}.{table_name}
                WHERE {taq_trade_bar_select_sql(tickers, restrict_to_exchanges)}
                GROUP BY
                  {grouping}"""
    return tbsql


def taq_trade_bars_sql(
    tickers: list[str] | str,
    date: datetime.date,
    bar_minutes: int = 30,
    group_by_exchange: bool = False,
    restrict_to_exchanges: tuple[str, ...] | str | None = None,
    include_first_and_last: bool = False,
    wrds_db: wrds.sql.Connection | None = None,
) -> str:
    date_str = date.strftime("%Y%m%d")
    year_str = date.strftime("%Y")
    db_name = f"taqm_{year_str}"
    table_name = f"ctm_{date_str}"

    if not include_first_and_last:
        bsql = taq_trade_bar_statistics_sql(
            tickers, date, bar_minutes, group_by_exchange, restrict_to_exchanges
        )
    else:
        # Latter years have a nanoseconds field
        db = get_wrds_connection(wrds_db)
        with HidePrinting():
            table_field_names = db.describe_table(library=db_name, table=table_name)

        # Latter years have a nanoseconds field
        nano_in_window = (
            "time_m_nano"
            if "time_m_nano" in set(table_field_names.name)
            else "0::smallint as time_m_nano"
        )

        fields = """  trade_stats_in_bar.ticker
                , trade_stats_in_bar.date
                , trade_stats_in_bar.window_time
                , vwap
                , last_trade_price
                , last_trade_size
                , last_trade_time
                , num_trades
                , total_qty
                , mean_price_ignoring_size
                , median_size
                , median_price
                , median_notional
                , min_price
                , max_price
                , min_size
                , max_size
                , first_trade_price
                , first_trade_size
                , first_trade_time
                , first_trade_time_ns
                , last_trade_time_ns"""

        order = "trade_stats_in_bar.ticker, trade_stats_in_bar.date, trade_stats_in_bar.window_time"

        if group_by_exchange:
            fields += "\n                , ex"
            order += ", trade_stats_in_bar.ex"
            first_last_distinct = "(ticker, date, hour_of_day, minute_of_hour, ex)"
            partition = f"{bar_sql(bar_minutes)}, ex"
        else:
            fields += (
                "\n                , first_trade_ex\n                , last_trade_ex"
            )
            first_last_distinct = "(ticker, date, hour_of_day, minute_of_hour)"
            partition = bar_sql(bar_minutes)

        bsql = f"""
            WITH 
              windowable_trades AS (
                SELECT
                    sym_root AS ticker
                    , date
                    , time_m
                    , {nano_in_window}
                    , sym_root
                    , EXTRACT(HOUR FROM time_m) AS hour_of_day
                    , {bar_minutes} * DIV(EXTRACT(MINUTE FROM time_m),{bar_minutes}) AS minute_of_hour
                    , ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY time_m DESC) AS rownum
                    , ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY time_m ASC) AS asc_rownum
                    , price
                    , size
                    , ex
                FROM {db_name}.{table_name}
                WHERE {taq_trade_bar_select_sql(tickers, restrict_to_exchanges)}
            ),
            last_trades AS (
              SELECT DISTINCT ON {first_last_distinct}
                    ticker
                    , date
                    , {window_time_sql(bar_minutes)} AS window_time
                    , time_m AS last_trade_time
                    , time_m_nano AS last_trade_time_ns
                    , price AS last_trade_price
                    , size as last_trade_size
                    , ex AS last_trade_ex
                FROM windowable_trades
                WHERE windowable_trades.rownum = 1
            ),
            first_trades AS (
              SELECT DISTINCT ON {first_last_distinct}
                    ticker
                    , date
                    , {window_time_sql(bar_minutes)} AS window_time
                    , time_m AS first_trade_time
                    , time_m_nano AS first_trade_time_ns
                    , price AS first_trade_price
                    , size as first_trade_size
                    , ex AS first_trade_ex
                FROM windowable_trades
                WHERE windowable_trades.asc_rownum = 1
            ),
            trade_stats_in_bar AS (
                {taq_trade_bar_statistics_sql(tickers, date, bar_minutes, group_by_exchange, restrict_to_exchanges)}
            )
            SELECT
                {fields}
            FROM trade_stats_in_bar 
                JOIN first_trades 
                  ON  {"trade_stats_in_bar.ex=first_trades.first_trade_ex" if group_by_exchange else "1=1"}
                  AND trade_stats_in_bar.ticker=first_trades.ticker 
                  AND trade_stats_in_bar.date=first_trades.date 
                  AND trade_stats_in_bar.window_time=first_trades.window_time
                JOIN last_trades 
                  ON  {"trade_stats_in_bar.ex=last_trades.last_trade_ex" if group_by_exchange else "1=1"}
                  AND trade_stats_in_bar.ticker=last_trades.ticker 
                  AND trade_stats_in_bar.date=last_trades.date 
                  AND trade_stats_in_bar.window_time=last_trades.window_time
            ORDER BY {order}"""
    return bsql


#########################
## Making WRDS Queries ##
#########################


def taq_trade_bars_on_date(
    tickers: list[str] | str,
    date: datetime.date,
    bar_minutes: int = 30,
    group_by_exchange: bool = False,
    restrict_to_exchanges: tuple[str] | None = None,
    include_first_and_last: bool = False,
    wrds_db: wrds.sql.Connection | None = None,
) -> pd.DataFrame:
    """
    Starting from 9:30AM NYC time and ending at 16:00, obtain bars of trade information
    as a Pandas dataframe by querying WRDS TAQ ctm_20??_* tables.

    No checking is done here for weekends, half days or holidays.  60 minute bars by necessity have one
    window that really only contains 30 minutes of data

    No support for symbol suffixes.

    Rookie alert: prices here are not dividend adjusted
    """
    db = get_wrds_connection(wrds_db)

    sql = taq_trade_bars_sql(
        tickers,
        date,
        bar_minutes,
        group_by_exchange,
        restrict_to_exchanges,
        include_first_and_last=include_first_and_last,
        wrds_db=db,
    )

    bars = cached_sql(db, sql)
    bars["window_time"] = pd.to_datetime(bars["window_time"]).dt.tz_localize(
        pytz.timezone("America/New_York")
    )

    if include_first_and_last:
        # Make timestamps Pythonic
        bars["last_trade_time"] = bars.apply(
            _make_timestamp, field_name_root="last_trade_time", axis=1
        )
        del bars["last_trade_time_ns"]
        bars["first_trade_time"] = bars.apply(
            _make_timestamp, field_name_root="first_trade_time", axis=1
        )
        del bars["first_trade_time_ns"]
    return bars


def taq_nbbo_bars_on_date(
    tickers: list[str] | str,
    date: datetime.date,
    bar_minutes: int = 30,
    wrds_db: wrds.sql.Connection | None = None,
) -> pd.DataFrame:
    """
    Starting from 9:30AM NYC time and ending at 16:00, obtain bars of national best bed and offer (NBBO)
    as a Pandas dataframe by querying WRDS TAQ complete_nbbo_* tables.

    No checking is done here for weekends, half days or holidays.  60 minute bars by necessity have one
    window that really only contains 30 minutes of data

    No support for symbol suffixes.

    Rookie alert: prices here are not dividend adjusted
    """
    assert bar_minutes == 60 or (bar_minutes <= 30 and 30 % bar_minutes == 0)
    assert bool(tickers)

    db = get_wrds_connection(wrds_db)

    if hasattr(tickers, "strip"):  # Allow single ticker as argument
        symbol_select = f"sym_root = '{tickers}'"
    elif len(tickers) == 1:
        symbol_select = f"sym_root = '{tickers[0]}'"
    else:
        symbol_select = f"sym_root IN {tuple(tickers)!r}"

    date_str = date.strftime("%Y%m%d")
    year_str = date.strftime("%Y")
    db_name = f"taqm_{year_str}"
    table_name = f"complete_nbbo_{date_str}"
    with HidePrinting():
        table_field_names = db.describe_table(library=db_name, table=table_name)

    # Latter years have a nanoseconds field
    nano_in_window = (
        "time_m_nano"
        if "time_m_nano" in set(table_field_names.name)
        else "0::smallint as time_m_nano"
    )

    sql = f"""
            WITH windowable_nbbo AS (
                SELECT
                    sym_root AS ticker
                    , date
                    , time_m
                    , {nano_in_window}
                    , sym_root
                    , qu_cond
                    , best_bid
                    , best_bidsizeshares
                    , best_ask
                    , best_asksizeshares
                    , EXTRACT(HOUR FROM time_m) AS hour_of_day
                    , {bar_minutes} * DIV(EXTRACT(MINUTE FROM time_m),{bar_minutes}) AS minute_of_hour
                    , ROW_NUMBER() OVER (PARTITION BY sym_root, EXTRACT(HOUR FROM time_m), DIV(EXTRACT(MINUTE FROM time_m),{bar_minutes}) ORDER BY time_m DESC) AS rownum
                FROM {db_name}.{table_name}
                WHERE 1=1
                  AND {symbol_select}
                  AND sym_suffix IS NULL
                  AND time_m > '09:30:00' AND time_m < '16:00:00'
            )
            SELECT DISTINCT ON (ticker, date, hour_of_day, minute_of_hour)
                ticker
                , date
                , {window_time_sql(bar_minutes)} AS window_time
                , best_bid
                , best_bidsizeshares
                , best_ask
                , best_asksizeshares
                , time_m AS time_of_last_quote
                , time_m_nano AS time_of_last_quote_ns
            FROM windowable_nbbo
            WHERE windowable_nbbo.rownum = 1
            """
    bars = cached_sql(db, sql)

    # Make timestamps Pythonic
    bars["time_of_last_quote"] = bars.apply(
        _make_timestamp, field_name_root="time_of_last_quote", axis=1
    )
    del bars["time_of_last_quote_ns"]
    bars["window_time"] = pd.to_datetime(bars["window_time"]).dt.tz_localize(
        pytz.timezone("America/New_York")
    )

    return bars
