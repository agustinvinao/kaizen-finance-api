queries = dict(
  {
    'list_assets_today': """SELECT a.symbol, a.exchange, a.category, 1 as bars_d, (now()::date - lb.start_at::date) * (24/4) as bars_4h
                            FROM assets a JOIN longterm_boundaries lb ON a.symbol = lb.symbol""",
    
    'list_assets_longterm_bars': """SELECT a.symbol, a.exchange, 
                                           (EXTRACT(days FROM (now() - lb.start_at)) / 7)::int + 10 as bars_w,
                                           (now()::date - lb.start_at::date) as bars_d,
                                           (now()::date - lb.start_at::date) * (24/4) as bars_4h
                                    FROM assets a JOIN longterm_boundaries lb ON a.symbol = lb.symbol """,
    
    'balance': """SELECT	
                    sum(p.amount) AS investment,
                    sum(p.quote_last_quote * p.shares) AS valuation,
                    sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 AS profits_hist_perc,
                    sum(p.quote_last_quote * p.shares) - sum(p.amount) AS profit_hist_value,
                    sum(ticks_1d_ago.close * p.shares) / sum(p.amount) - 1 AS profits_1d_ago_perc,
                    sum(ticks_1d_ago.close * p.shares) - sum(p.amount) AS profit_1d_ago_value
                  FROM assets a
                    INNER JOIN positions p ON a.symbol = p.symbol
                    INNER JOIN LATERAL (
                        SELECT close, dt FROM ticks_1d t1d
                          WHERE p.symbol = t1d.symbol AND t1d.dt < CURRENT_DATE ORDER BY dt DESC LIMIT 1
                      ) AS ticks_1d_ago ON true
                  WHERE a.category = 'stock' AND p.shares > 0""",

   'positions_status': """SELECT
                            p.symbol,
                            p.amount AS amount,
                            p.quote_purchase AS quote_purchase,
                            p.quote_last_quote AS quote_last_quote, ticks_1d_ago.close AS ticks_yesterday,
                            ((p.quote_last_quote * p.shares) - p.amount) AS profit_historical,
                            p.quote_last_quote/p.quote_purchase - 1 AS profit_historical_perc,
                            p.shares * (ticks_1d_ago.close - p.quote_last_quote) AS profits_last_day,
                            p.quote_last_quote/ticks_1d_ago.close - 1 AS profits_last_day_perc,
                            p.amount/p_tot.total AS participation
                          FROM assets a
                            INNER JOIN positions p ON a.symbol = p.symbol
                            INNER JOIN LATERAL (
                                SELECT close, dt FROM ticks_1d t1d
                                  WHERE p.symbol = t1d.symbol AND t1d.dt < CURRENT_DATE ORDER BY dt DESC LIMIT 1
                              ) AS ticks_1d_ago ON true
                            JOIN LATERAL (SELECT SUM(amount) AS total FROM positions) AS p_tot ON true
                          WHERE a.category = 'stock' AND p.shares > 0
                          ORDER BY (p.quote_last_quote/ticks_1d_ago.close - 1)""",

    'crossing_levels': """SELECT 	to_char(now() - p.updated_at, 'dd "days" ago, hh24 "hours" ago') AS ago,
                            p.symbol,
                            t1d.dt::date AS dt,
                            la.level / 100 as level,
                            CASE
                                    WHEN t1d.close > t1d.open THEN 'bullish'
                                    WHEN t1d.open > t1d.close THEN 'bearish'
                                END AS direction,
                                CASE
                                    WHEN la.price BETWEEN t1d.close AND t1d.open THEN 'annual'
                                    WHEN la.price BETWEEN t1d.open AND t1d.close THEN 'annual'
                                END AS crossing,
                                la.dist,
                                p.quote_last_quote / la.price - 1 AS movement
                        FROM positions p
                          CROSS JOIN LATERAL (SELECT *, CASE WHEN t1d.open > t1d.close THEN 'desc' WHEN t1d.open < t1d.close THEN 'asc' END AS direction
                                    FROM ticks_1d t1d WHERE p.symbol = t1d.symbol ORDER BY dt DESC LIMIT 1) t1d
                          CROSS JOIN LATERAL (SELECT la.open_price, la.price, la.level, abs(t1d.close - la.price) / t1d.close AS dist
                                        FROM levels_annual la WHERE p.symbol = la.symbol ORDER BY dist LIMIT 1) la
                        WHERE (la.price BETWEEN t1d.close AND t1d.open) OR (la.price BETWEEN t1d.open AND t1d.close)
                        UNION
                        SELECT 	to_char(now() - p.updated_at, 'dd "days" ago, hh24 "hours" ago') AS ago,
                            p.symbol,
                            t1d.dt::date AS dt,
                            ll.level,
                            CASE
                                    WHEN t1d.close > t1d.open THEN 'bullish'
                                    WHEN t1d.open > t1d.close THEN 'bearish'
                                END AS direction,
                                CASE
                                    WHEN ll.price BETWEEN t1d.close AND t1d.open THEN 'longterm'
                                    WHEN ll.price BETWEEN t1d.open AND t1d.close THEN 'longterm'
                                END AS crossing,
                                ll.dist,
                                p.quote_last_quote / ll.price - 1 AS movement
                        FROM positions p
                          CROSS JOIN LATERAL (SELECT *, CASE WHEN t1d.open > t1d.close THEN 'desc' WHEN t1d.open < t1d.close THEN 'asc' END AS direction
                                    FROM ticks_1d t1d WHERE p.symbol = t1d.symbol ORDER BY dt DESC LIMIT 1) t1d
                          CROSS JOIN LATERAL (SELECT ll.price, ll.level, abs(t1d.close - ll.price) / t1d.close AS dist
                                        FROM levels_longterm ll WHERE p.symbol = ll.symbol ORDER BY dist LIMIT 1) ll
                        WHERE (ll.price BETWEEN t1d.close AND t1d.open) OR (ll.price BETWEEN t1d.open AND t1d.close)
                        ORDER BY direction DESC, symbol"""
  }
)