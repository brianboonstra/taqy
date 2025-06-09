SELECT
                    sym_root AS ticker
                    , date
                    , date + (1 + EXTRACT(HOUR FROM time_m) || ':00')::interval AS window_time
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
                FROM taqm_2024.ctm_20240229 
                WHERE ex IN ('D', 'P')
                  AND sym_root IN ('SPY', 'JPM', 'LLY')
                  AND sym_suffix IS NULL
                  AND time_m > '09:30:00' AND time_m < '16:00:00'
                GROUP BY
                  sym_root, date, EXTRACT(HOUR FROM time_m), DIV(EXTRACT(MINUTE FROM time_m),60)