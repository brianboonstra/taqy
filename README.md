# TAQY

The `taqy` project is Python library for accessing and summarizing time-and-quote information.  It's initial remit is to generate more manageable “bars” from Wharton Research Data Services (WRDS) subsecond-level trade, bid, and offer US equity data based on the NYSE TAQ. 

## Motivation
Downloading full market data (hundreds of millions of rows) is often impractical. Instead, this repository shows an approach to generate price “bars” on the server side using SQL queries.  While many people and LLMs can write SQL after a fashion, our particular use case demands somewhat carefully architected SQL, taking advantage of row counting tricks and the like.  Rather than requiring academics and nonexperts learn SQL to that level, I provide this library.

The essence of this library is expressed in two functions, described in greater detail below:

• taq_nbbo_bars_on_date()  
• taq_trade_bars_on_date()

For more detailed information about the NYSE TAQ data set, please refer to the official documentation:  
https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf

## Installation

You can install this library with `pip install taqy`

## Usage

### Connecting to WRDS

In order access the WRDS TAQ database, you need to have a valid WRDS account.  This will give you API access via SQL calls to their PostgreSQL server.

For convenience, you can manage your own database connection object, or you can use the lightweight wrapper provided by `taqy`, making database connections a little more invisible.  In the examples below, I presume you have done the latter.

### NBBO Bars

```python
import datetime
import taqy
import wrds

# Connect to WRDS (You must have credentials set up)
wrds_db = wrds.Connection()

# Example usage for retrieving NBBO bars
df_nbbo = taqy.taq_nbbo_bars_on_date(
    tickers=["AAPL", "GOOG"],
    date=datetime.date(2023, 8, 1),
    bar_minutes=30,
    wrds_db=wrds_db
)

print(df_nbbo.head())
```

### Trade Bars

```python
import datetime
import taqy
import wrds

# Connect to WRDS (You must have credentials set up)
wrds_db = wrds.Connection()

# Example usage for retrieving trade bars
df_trade = taqy.taq_trade_bars_on_date(
    tickers=["AAPL", "GOOG"],
    date=datetime.date(2023, 8, 1),
    bar_minutes=30,
    group_by_exchange=True,
    wrds_db=wrds_db
)

print(df_trade.head())
```

(Additional sections can go here, such as: Data Schema, Contributors, License, etc.)