# TAQY

The `taqy` project is Python library for accessing and summarizing time-and-quote information.  Its initial remit is to generate more manageable “bars” from Wharton Research Data Services (WRDS) subsecond-level trade, bid, and offer US equity data based on the NYSE TAQ.

_Brian K. Boonstra_, 2025

## Motivation
Downloading full market data (hundreds of millions of rows) is often impractical. Instead, this repository employs an approach to generate price “bars” on the server side using SQL queries.  While many people and LLMs can write SQL after a fashion, our particular use case demands somewhat carefully architected SQL.  To get decent performance we must take advantage of row counting tricks and the like.  Rather than expecting academics and nonexperts learn SQL to that level, I provide this library.

The essence of this library is expressed in two functions, described in greater detail below:

• taq_nbbo_bars_on_date()  
• taq_trade_bars_on_date()

For more information on the WRDS data service, please see https://wrds-www.wharton.upenn.edu.

For more detailed information about the NYSE TAQ data set, please refer to the official documentation:  
https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf

## Installation

You can install this library with `pip install taqy`. A conda version is in the works.

## Usage

### General Commentary

The most important thing to know here is that our data from WRDS is _not_ dividend and split adjusted.  That is standard for intraday data sets, but means you will need to make adjustments if you are joining this data with commonly used adjusted open/close data sets.

Though WRDS does provide information about trading halts and limit-up/limit-down conditions, there is no support (at present) for making use of that information in `taqy` itself.  We also do not presently filter on quote conditions found in the `qu_cond` field.

Please note that windows without any data will be _absent_ from the dataframes you obtain here.  If you have 1 minute bars and PBPB did not trade for 6 minutes, you will have 6 "missing" bars in the output.

Though the routines here are as efficient as I can make them, calls usually take many seconds to complete.  Though this API allows for many tickers and for bar windows of just one minute, you will generally find that big data queries are still cumbersome or subject to timeouts.

At present, we only support tickers without suffixes.  Enhancing the code to support suffixes is not hard, so it will probably be finished some day.

WRDS began providing nanosecond-level detail in its timestamps a few years ago.   `taqy` incorporates the nanosecond information when it is available.

### Connecting to WRDS

In order access the WRDS TAQ database, you need to have a valid WRDS account.  This will give you API access via SQL calls to their PostgreSQL server.  Doing so involves having a "Connection" object available in your Python session.

For convenience, you can manage your own database connection object and use the `wrds_db` argument, *or* you can use the lightweight wrapper provided by `taqy`, making database connections a little more convenient.  In the examples below, I presume you have done the latter.  Here is how you start it:

```python
from taqy.usequity import connect_to_wrds

connect_to_wrds()
```

### NBBO Bars

National best bid and offer (NBBO) bars tell us, for each bar window timestamp, what the price and size of the best bid and offer were across all US exchanges.  We also obtain the time of the most recent quote (though not in this case whether it came from a bid update or offer update).

At present, `bar_minutes` must either evenly divide 30, or it must be 60.  In the case of 60 minute bars, one of the windows really only has half an hour of data since US trading hours are 09:30-16:00 New York time.

For convenience, the `tickers` argument may be a list or a single string containing the one ticker of interest.

```python
import datetime
from taqy.usequity import taq_nbbo_bars_on_date

nbbo_bars = taq_nbbo_bars_on_date(
    tickers=['SPY', 'PBPB', 'HLIT'],
    date=datetime.date(2024,2,29),
    bar_minutes=60
).set_index(['ticker', 'date', 'window_time']).unstack(0)
```



<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th></th>
      <th colspan="3" halign="left">best_bid</th>
      <th colspan="3" halign="left">best_bidsizeshares</th>
      <th colspan="3" halign="left">best_ask</th>
      <th colspan="3" halign="left">best_asksizeshares</th>
      <th colspan="3" halign="left">time_of_last_quote</th>
    </tr>
    <tr>
      <th></th>
      <th>ticker</th>
      <th>HLIT</th>
      <th>PBPB</th>
      <th>SPY</th>
      <th>HLIT</th>
      <th>PBPB</th>
      <th>SPY</th>
      <th>HLIT</th>
      <th>PBPB</th>
      <th>SPY</th>
      <th>HLIT</th>
      <th>PBPB</th>
      <th>SPY</th>
      <th>HLIT</th>
      <th>PBPB</th>
      <th>SPY</th>
    </tr>
    <tr>
      <th>date</th>
      <th>window_time</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="7" valign="top">2024-02-29</th>
      <th>2024-02-29 10:00:00-05:00</th>
      <td>13.26</td>
      <td>13.74</td>
      <td>508.49</td>
      <td>600</td>
      <td>100</td>
      <td>200</td>
      <td>13.27</td>
      <td>13.78</td>
      <td>508.5</td>
      <td>300</td>
      <td>100</td>
      <td>400</td>
      <td>2024-02-29 09:59:55.164173375-05:00</td>
      <td>2024-02-29 09:59:59.670322279-05:00</td>
      <td>2024-02-29 09:59:59.979579648-05:00</td>
    </tr>
    <tr>
      <th>2024-02-29 11:00:00-05:00</th>
      <td>13.13</td>
      <td>13.75</td>
      <td>507.44</td>
      <td>500</td>
      <td>300</td>
      <td>200</td>
      <td>13.14</td>
      <td>13.8</td>
      <td>507.45</td>
      <td>500</td>
      <td>400</td>
      <td>300</td>
      <td>2024-02-29 10:59:56.719858721-05:00</td>
      <td>2024-02-29 10:58:52.825894793-05:00</td>
      <td>2024-02-29 10:59:59.993879808-05:00</td>
    </tr>
    <tr>
      <th>2024-02-29 12:00:00-05:00</th>
      <td>13.01</td>
      <td>13.81</td>
      <td>506.65</td>
      <td>700</td>
      <td>400</td>
      <td>300</td>
      <td>13.02</td>
      <td>13.84</td>
      <td>506.66</td>
      <td>800</td>
      <td>200</td>
      <td>500</td>
      <td>2024-02-29 11:59:51.922733889-05:00</td>
      <td>2024-02-29 11:59:43.408821488-05:00</td>
      <td>2024-02-29 11:59:59.999457024-05:00</td>
    </tr>
    <tr>
      <th>2024-02-29 13:00:00-05:00</th>
      <td>13.05</td>
      <td>13.84</td>
      <td>507.36</td>
      <td>1100</td>
      <td>400</td>
      <td>300</td>
      <td>13.06</td>
      <td>13.87</td>
      <td>507.37</td>
      <td>400</td>
      <td>100</td>
      <td>300</td>
      <td>2024-02-29 12:59:51.822595396-05:00</td>
      <td>2024-02-29 12:59:56.123094269-05:00</td>
      <td>2024-02-29 12:59:59.995182080-05:00</td>
    </tr>
    <tr>
      <th>2024-02-29 14:00:00-05:00</th>
      <td>13.09</td>
      <td>13.85</td>
      <td>507.8</td>
      <td>1000</td>
      <td>400</td>
      <td>200</td>
      <td>13.1</td>
      <td>13.88</td>
      <td>507.81</td>
      <td>1100</td>
      <td>200</td>
      <td>500</td>
      <td>2024-02-29 13:59:59.601319642-05:00</td>
      <td>2024-02-29 13:58:04.797251444-05:00</td>
      <td>2024-02-29 13:59:59.947843072-05:00</td>
    </tr>
    <tr>
      <th>2024-02-29 15:00:00-05:00</th>
      <td>13.11</td>
      <td>13.86</td>
      <td>507.74</td>
      <td>500</td>
      <td>200</td>
      <td>2200</td>
      <td>13.12</td>
      <td>13.88</td>
      <td>507.75</td>
      <td>700</td>
      <td>200</td>
      <td>300</td>
      <td>2024-02-29 14:59:55.053310002-05:00</td>
      <td>2024-02-29 14:59:50.046429293-05:00</td>
      <td>2024-02-29 14:59:59.978044672-05:00</td>
    </tr>
    <tr>
      <th>2024-02-29 16:00:00-05:00</th>
      <td>13.13</td>
      <td>13.88</td>
      <td>507.99</td>
      <td>28200</td>
      <td>1000</td>
      <td>108900</td>
      <td>13.14</td>
      <td>13.89</td>
      <td>508.0</td>
      <td>1100</td>
      <td>300</td>
      <td>53700</td>
      <td>2024-02-29 15:59:59.836964072-05:00</td>
      <td>2024-02-29 15:59:59.743245800-05:00</td>
      <td>2024-02-29 15:59:59.999648512-05:00</td>
    </tr>
  </tbody>
</table>


### Trade Bars


Trade bars tell us, for each bar window timestamp, simple statistics that occurred in the window preceding the timestamp.

It is optional to include information about the first and last trades within the window via `include_first_and_last`, since doing so gives us a noticeably more complex SQL query.

At present, `bar_minutes` must either evenly divide 30, or it must be 60.  As with NBBO, one of the 60 minute bars will only come from 30 minutes of data.

For convenience, the `tickers` argument may be a list or a single string containing the one ticker of interest.  Similarly, `restrict_to_exchanges` can be a string denoting a single exchange.

By default, trades will be summarized across all exchanges (or the ones specified by `restrict_to_exchanges`) in the dataset.  By toggling `group_by_exchange`, your summary statistics will instead come back grouped by exchange.

Please note the exchange is provided by the column `ex` in the dataframe.

```python
import datetime
from taqy.usequity import taq_trade_bars_on_date

trade_bars = taq_trade_bars_on_date(
    ['SPY', 'PBPB', 'HLIT'], 
    date=datetime.date(2024,2,29), 
    group_by_exchange=True,
    restrict_to_exchanges=('M','P'),
    include_first_and_last=True
).transpose()

```

<table border="1" class="dataframe">
  <tbody>
    <tr>
      <th>ticker</th>
      <td>HLIT</td>
      <td>HLIT</td>
      <td>HLIT</td>
      <td>SPY</td>
      <td>SPY</td>
      <td>SPY</td>
    </tr>
    <tr>
      <th>date</th>
      <td>2024-02-29 00:00:00</td>
      <td>2024-02-29 00:00:00</td>
      <td>2024-02-29 00:00:00</td>
      <td>2024-02-29 00:00:00</td>
      <td>2024-02-29 00:00:00</td>
      <td>2024-02-29 00:00:00</td>
    </tr>
    <tr>
      <th>window_time</th>
      <td>2024-02-29 10:00:00-05:00</td>
      <td>2024-02-29 10:30:00-05:00</td>
      <td>2024-02-29 11:00:00-05:00</td>
      <td>2024-02-29 15:30:00-05:00</td>
      <td>2024-02-29 16:00:00-05:00</td>
      <td>2024-02-29 16:00:00-05:00</td>
    </tr>
    <tr>
      <th>vwap</th>
      <td>13.22803</td>
      <td>13.133143</td>
      <td>13.130639</td>
      <td>507.914929</td>
      <td>508.810897</td>
      <td>509.067939</td>
    </tr>
    <tr>
      <th>last_trade_price</th>
      <td>13.25</td>
      <td>13.1</td>
      <td>13.13</td>
      <td>508.14</td>
      <td>509.39</td>
      <td>508.0</td>
    </tr>
    <tr>
      <th>last_trade_size</th>
      <td>100</td>
      <td>13</td>
      <td>16</td>
      <td>84</td>
      <td>41</td>
      <td>900</td>
    </tr>
    <tr>
      <th>last_trade_time</th>
      <td>2024-02-29 09:57:34.293454515-05:00</td>
      <td>2024-02-29 10:29:17.202749055-05:00</td>
      <td>2024-02-29 10:59:46.808839585-05:00</td>
      <td>2024-02-29 15:29:59.696625664-05:00</td>
      <td>2024-02-29 15:59:01.865104640-05:00</td>
      <td>2024-02-29 15:59:59.988947456-05:00</td>
    </tr>
    <tr>
      <th>num_trades</th>
      <td>55</td>
      <td>122</td>
      <td>80</td>
      <td>4800</td>
      <td>21</td>
      <td>27174</td>
    </tr>
    <tr>
      <th>total_qty</th>
      <td>2036</td>
      <td>6593</td>
      <td>6414</td>
      <td>469365</td>
      <td>524</td>
      <td>5716498</td>
    </tr>
    <tr>
      <th>mean_price_ignoring_size</th>
      <td>13.226364</td>
      <td>13.137541</td>
      <td>13.134937</td>
      <td>507.907977</td>
      <td>508.63381</td>
      <td>508.989108</td>
    </tr>
    <tr>
      <th>median_size</th>
      <td>25.0</td>
      <td>33.5</td>
      <td>56.5</td>
      <td>100.0</td>
      <td>5.0</td>
      <td>100.0</td>
    </tr>
    <tr>
      <th>median_price</th>
      <td>13.23</td>
      <td>13.12</td>
      <td>13.14</td>
      <td>507.89</td>
      <td>508.58</td>
      <td>508.97</td>
    </tr>
    <tr>
      <th>median_notional</th>
      <td>330.5</td>
      <td>439.52</td>
      <td>742.96</td>
      <td>50780.0</td>
      <td>2543.75</td>
      <td>50910.0</td>
    </tr>
    <tr>
      <th>min_price</th>
      <td>13.19</td>
      <td>13.09</td>
      <td>13.08</td>
      <td>507.65</td>
      <td>508.15</td>
      <td>507.94</td>
    </tr>
    <tr>
      <th>max_price</th>
      <td>13.27</td>
      <td>13.28</td>
      <td>13.17</td>
      <td>508.19</td>
      <td>509.44</td>
      <td>509.74</td>
    </tr>
    <tr>
      <th>min_size</th>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>max_size</th>
      <td>100</td>
      <td>600</td>
      <td>500</td>
      <td>1178</td>
      <td>100</td>
      <td>16041</td>
    </tr>
    <tr>
      <th>first_trade_price</th>
      <td>13.2</td>
      <td>13.27</td>
      <td>13.12</td>
      <td>507.75</td>
      <td>508.15</td>
      <td>508.14</td>
    </tr>
    <tr>
      <th>first_trade_size</th>
      <td>100</td>
      <td>200</td>
      <td>22</td>
      <td>100</td>
      <td>1</td>
      <td>93</td>
    </tr>
    <tr>
      <th>first_trade_time</th>
      <td>2024-02-29 09:30:00.092696390-05:00</td>
      <td>2024-02-29 10:00:00.737432736-05:00</td>
      <td>2024-02-29 10:30:11.467083039-05:00</td>
      <td>2024-02-29 15:00:00.011126272-05:00</td>
      <td>2024-02-29 15:30:03.499381248-05:00</td>
      <td>2024-02-29 15:30:00.000087808-05:00</td>
    </tr>
    <tr>
      <th>ex</th>
      <td>P</td>
      <td>P</td>
      <td>P</td>
      <td>P</td>
      <td>M</td>
      <td>P</td>
    </tr>
  </tbody>
</table>

## Implementation Notes

### WRDS Tables

The WRDS database has both yearly and daily tables.  Given the density of data we deal with here, it makes the most sense to query the daily tables with loop rather than a giant query across many dates.

The tables WRDS provides include our main tables of interest `complete_nbbo_*` and `ctm_*`.  Tables presently unused include `cqm_*`, which is all quotes (but not enough information to really "build the book").  `nbbom_*` is best bid and offer by exchange, `luld_*` is limit up limit down.  `mastm_*` is the "master" table, including special information like trading halts.

```python
', '.join([t for t in DEFAULT_WRDS_CONNECTION.list_tables(library='taqm_2024') if '2024' not in t or '20240124' in t or t.endswith('2024')])

complete_nbbo_2024, complete_nbbo_20240124, cqm_2024, cqm_20240124, ctm_2024, ctm_20240124, luld_cqm_2024, luld_cqm_20240124, luld_ctm_2024, luld_ctm_20240124, mastm_2024, mastm_20240124, nbbom_2024, nbbom_20240124, wct_2024, wct_20240124, wrds_iid_2024
```

### Development Notes

#### Developer Usage

Developers may find themselves preferring to use the `wrds_db` argument in order to have better control over connectivity.  The trick of a connection in a global variable in contrast is very convenient for researchers.

Any time you want to avoid using the database access cache, or clear out the cache, you can manipulate the `taqy.usequity.CACHED_QUERIES` variable.

#### Testing

##### Running Tests
You can run tests with `pytest tests`.

The testing routines here do _not_ usually query WRDS.  Instead they mock out the SQL queries with a substitute that queries the filesystem, specifically the entries in `tests/mock_sql_responses`.  For someone who wanted to enhance `taqy` with customary usage of a filesystem cache, the code controlling this in `conftest.py` may provide useful guidance.

In order to update or add a test, you can run `pytest` with an `--avoid-db-cache` argument.  Once satisfied with base DB access, you should then run with `--write-db-cache`.

Regression test output can be saved or updated with the argument `--update-gold`.

##### Consistency Checks

In addition to fairly standard regression tests, the tests here also include checks on consistency.  Here, we check:
  - multi-ticker queries agree with single-ticker queries for NBBO bars
  - trade bars asking for fist and last trade information (complicated SQL) agree with trade bars not asking for extra information
  - trade bars that are K minutes agree with trade bars of length N*K minutes about total trade count, etc.

### Choices of Statistics

The trade statistics chosen here would be quite easy to enhance with, e.g., deciles, or any other aggregate function natively supported by the WRDS PostgreSQL. 

### SQL Implementation

For a lot of people, it is easiest to use simple SQL queries and then employ _pandas_ `groupby()` or similar routines to work out the bars.  However, sending these large datasets over the internet makes that practice infeasible in our case.

Here, I make heavy use of common table expressions (`WITH ...`) to keep queries efficient and as clear as I can.  The NBBO query uses a CTE that extracts row numbers in descending order within each window, which lets us identify the last quote before our window time by just choosing row number 1.  S `SELECT DISTINCT` then allow the PostgreSQL query optimizer to short-circuit quickly with the data we need.

Because the trade bars want summary statistics over entire window interval, no such short circuit trick is feasible for trades and I just use `GROUP BY`.  This is not as bad as it seems, since trade data is generally 10-100 times less voluminous than quote data.

When first and last trade information is requested (by the `include_first_and_last` flag), I again use windowed row numbering to find the relevant data, along with CTE expressions to construct the final query.

#### SQL Examples

Here are examples of SQL queries sent by `taqy` to WRDS:

##### SQL Example: NBBO Bars

```SQL
            WITH windowable_nbbo AS (
                SELECT
                    sym_root AS ticker
                    , date
                    , time_m
                    , time_m_nano
                    , sym_root
                    , qu_cond
                    , best_bid
                    , best_bidsizeshares
                    , best_ask
                    , best_asksizeshares
                    , EXTRACT(HOUR FROM time_m) AS hour_of_day
                    , 1 * DIV(EXTRACT(MINUTE FROM time_m),1) AS minute_of_hour
                    , ROW_NUMBER() OVER (PARTITION BY sym_root, EXTRACT(HOUR FROM time_m), DIV(EXTRACT(MINUTE FROM time_m),1) ORDER BY time_m DESC) AS rownum
                FROM taqm_2024.complete_nbbo_20240229
                WHERE 1=1
                  AND sym_root = 'LLY'
                  AND sym_suffix IS NULL
                  AND time_m > '09:30:00' AND time_m < '16:00:00'
            )
            SELECT DISTINCT ON (ticker, date, hour_of_day, minute_of_hour)
                ticker
                , date
                , date + (EXTRACT(HOUR FROM time_m) || ':' || 1 * DIV(EXTRACT(MINUTE FROM time_m),1))::interval + ( '00:1' )::interval AS window_time
                , best_bid
                , best_bidsizeshares
                , best_ask
                , best_asksizeshares
                , time_m AS time_of_last_quote
                , time_m_nano AS time_of_last_quote_ns
            FROM windowable_nbbo
            WHERE windowable_nbbo.rownum = 1
```

##### SQL Example: Trade Bars

```sql
            WITH 
              windowable_trades AS (
                SELECT
                    sym_root AS ticker
                    , date
                    , time_m
                    , time_m_nano
                    , sym_root
                    , EXTRACT(HOUR FROM time_m) AS hour_of_day
                    , 30 * DIV(EXTRACT(MINUTE FROM time_m),30) AS minute_of_hour
                    , ROW_NUMBER() OVER (PARTITION BY sym_root, date, EXTRACT(HOUR FROM time_m), DIV(EXTRACT(MINUTE FROM time_m),30), ex ORDER BY time_m DESC) AS rownum
                    , ROW_NUMBER() OVER (PARTITION BY sym_root, date, EXTRACT(HOUR FROM time_m), DIV(EXTRACT(MINUTE FROM time_m),30), ex ORDER BY time_m ASC) AS asc_rownum
                    , price
                    , size
                    , ex
                FROM taqm_2024.ctm_20240229
                WHERE ex IN ('M', 'P')
                  AND sym_root IN ('SPY', 'PBPB', 'HLIT')
                  AND sym_suffix IS NULL
                  AND time_m > '09:30:00' AND time_m < '16:00:00'
            ),
            last_trades AS (
              SELECT DISTINCT ON (ticker, date, hour_of_day, minute_of_hour, ex)
                    ticker
                    , date
                    , date + (EXTRACT(HOUR FROM time_m) || ':' || 30 * DIV(EXTRACT(MINUTE FROM time_m),30))::interval + ( '00:30' )::interval AS window_time
                    , time_m AS last_trade_time
                    , time_m_nano AS last_trade_time_ns
                    , price AS last_trade_price
                    , size as last_trade_size
                    , ex AS last_trade_ex
                FROM windowable_trades
                WHERE windowable_trades.rownum = 1
            ),
            first_trades AS (
              SELECT DISTINCT ON (ticker, date, hour_of_day, minute_of_hour, ex)
                    ticker
                    , date
                    , date + (EXTRACT(HOUR FROM time_m) || ':' || 30 * DIV(EXTRACT(MINUTE FROM time_m),30))::interval + ( '00:30' )::interval AS window_time
                    , time_m AS first_trade_time
                    , time_m_nano AS first_trade_time_ns
                    , price AS first_trade_price
                    , size as first_trade_size
                    , ex AS first_trade_ex
                FROM windowable_trades
                WHERE windowable_trades.asc_rownum = 1
            ),
            trade_stats_in_bar AS (
                SELECT
                    sym_root AS ticker
                    , date
                    , date + (EXTRACT(HOUR FROM time_m) || ':' || 30 * DIV(EXTRACT(MINUTE FROM time_m),30))::interval + ( '00:30' )::interval AS window_time
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
                    , MIN(size) AS min_size
                    , ex
                FROM taqm_2024.ctm_20240229 
                WHERE ex IN ('M', 'P')
                  AND sym_root IN ('SPY', 'PBPB', 'HLIT')
                  AND sym_suffix IS NULL
                  AND time_m > '09:30:00' AND time_m < '16:00:00'
                GROUP BY
                  sym_root, date, EXTRACT(HOUR FROM time_m), DIV(EXTRACT(MINUTE FROM time_m),30), ex
            )
            SELECT
                  trade_stats_in_bar.ticker
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
                , last_trade_time_ns
                , ex
            FROM trade_stats_in_bar 
                JOIN first_trades 
                  ON  trade_stats_in_bar.ex=first_trades.first_trade_ex
                  AND trade_stats_in_bar.ticker=first_trades.ticker 
                  AND trade_stats_in_bar.date=first_trades.date 
                  AND trade_stats_in_bar.window_time=first_trades.window_time
                JOIN last_trades 
                  ON  trade_stats_in_bar.ex=last_trades.last_trade_ex
                  AND trade_stats_in_bar.ticker=last_trades.ticker 
                  AND trade_stats_in_bar.date=last_trades.date 
                  AND trade_stats_in_bar.window_time=last_trades.window_time
            ORDER BY trade_stats_in_bar.ticker, trade_stats_in_bar.date, trade_stats_in_bar.window_time, trade_stats_in_bar.ex
```