query_assets_by_timeframe_and_exchange = """SELECT a.symbol, a.exchange, etz.tz, ao.tf, 'ticks_' || ao.tf AS table, bars.default_bars
  FROM assets a
    JOIN assets_observability ao ON a.symbol = ao.symbol
    JOIN exchanges_tz etz ON a.exchange = etz.exchange
    JOIN (SELECT * FROM (
            VALUES ('4h'::timeframe, 28),('1d'::timeframe, 5),('1w'::timeframe, 5)
          ) AS t (tf, default_bars)
    ) bars ON ao.tf = bars.tf
  WHERE ao.tf = '{timeframe}'"""
filter_by_exchanges = """ AND a.exchange IN ('{exchanges}')"""

query_assets_by_timeframe_and_exchange_without_update = """SELECT a.symbol, a.exchange, ao.tf, 'ticks_' || ao.tf AS table, bars.default_bars
FROM assets a
	JOIN last_ticks_{timeframe} lt ON	a.symbol = lt.symbol
  JOIN assets_observability ao ON lt.symbol = ao.symbol
  JOIN exchanges_tz etz ON a.exchange = etz.exchange
	JOIN (SELECT * FROM (
            VALUES ('4h'::timeframe, 28),('1d'::timeframe, 5),('1w'::timeframe, 5)
          ) AS t (tf, default_bars)
    ) bars ON ao.tf = bars.tf
	JOIN (SELECT max(dt) AS max_dt FROM last_ticks_{timeframe}) lt_max ON true
WHERE ao.tf = '{timeframe}' AND lt.dt < lt_max.max_dt"""

query_asset_lookup = """SELECT a.symbol, a.exchange, bars.tf, 'ticks_' || bars.tf AS table, bars.default_bars
  FROM (SELECT * FROM (VALUES ('{}', '{}')) AS t (symbol, exchange)) AS a
    	JOIN (SELECT * FROM (VALUES ('4h'::timeframe, 28),('1d'::timeframe, 5),('1w'::timeframe, 5)) AS t (tf, default_bars)) bars ON true """
     
query_asset_by_symbol = """SELECT a.*, etz.tz FROM assets a JOIN exchanges_tz etz ON a.exchange = etz.exchange  WHERE symbol='{}'"""