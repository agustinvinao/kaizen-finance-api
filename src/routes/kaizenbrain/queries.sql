-- SMA 20 dias
SELECT
  bucket,
  avg(close) OVER (order by bucket ROWS BETWEEN 19 PRECEDING AND current ROW) AS sma_20
FROM daily_bars
WHERE symbol=''
ORDER BY bucket DESC;

-- Stock de mayor ganancia por dia
WITH prev_day_closing AS (
	SELECT symbol, bucket, close,
		LEAD(close) OVER (PARTITION BY symbol ORDER BY bucket DESC) AS prev_day_closing_quote
	FROM daily_bars
), daily_factor AS (
	SELECT symbol, bucket, close / prev_day_closing_quote AS daily_factor
	FROM prev_day_closing
)
SELECT
	bucket,
	LAST(symbol, daily_factor) AS symbol,
	MAX(daily_factor) as max_daily_factor
FROM daily_factor JOIN securities ON securities.id = daily_factor.symbol
GROUP BY bucket
ORDER BY bucket DESC, max_daily_factor DESC

-- listar acciones que cierran con valor menor al valor de la hora anterior
SELECT * FROM (
	SEELCT symbol, dt, close,
		LEAD(close, 1) over (PARTITION BY symbol ORDER BY dt DESC) AS previous_close
	from ticks
) a
WHERE close < previous_close AND extract(HOUR FROM dt) = 20 AND extract(YEAR FROM dt) = 2024
ORDER BY dt DESC

-- bullish engulfing
SELECT * FROM (
	SELECT
		bucket, open, close, symbol,
		LAG(close, 1) OVER (
			PARTITION BY symbol
			ORDER BY bucket
		) previous_close,
		LAG(open, 1) OVER (
			PARTITION BY symbol
			ORDER BY bucket
		) previous_open
	FROM daily_bars
) a
WHERE previous_close < previous_open AND close > previous_open AND open < previous_close
ORDER BY bucket DESC

-- cierre mayor en los ultimos 3 dias
SELECT * FROM (
	SELECT
		bucket,
		close,
		volume,
		symbol,
		LAG(close, 1) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_close,
		LAG(volume, 1) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_volume,
		LAG(close, 2) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_previous_close,
		LAG(volume, 2) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_previous_volume
	FROM daily_bars
) a
WHERE close > previous_close AND previous_close > previous_previous_close
AND volume > previous_volume AND previous_volume > previous_previous_close
ORDER BY bucket DESC

-- 3 barras descendientes seguidas de una ascendente que supere el volumen de las anteriores
SELECT * FROM (
	SELECT
		bucket,
		close,
		volume,
		symbol,
		LAG(close,  1) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_close,
		LAG(volume, 1) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_volume,
		LAG(close,  2) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_previous_close,
		LAG(volume, 2) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_previous_volume,
    LAG(close,  3) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_previous_previous_close,
		LAG(volume, 3) OVER ( PARTITION BY symbol ORDER BY bucket ) previous_previous_previous_volume
	FROM daily_bars where extract(year from bucket) = 2024
) a
WHERE
	close > previous_previous_close AND
	previous_close < previous_previous_close AND
	previous_previous_close < previous_previous_previous_close AND
	volume > previous_volume AND
	previous_close < previous_previous_close AND
	previous_previous_close < previous_previous_previous_close


-- detalle posiciones
SELECT
	p.symbol, p.amount, p.quote_purchase, p.quote_last_quote,
	p.quote_last_quote/p.quote_purchase - 1 AS profits_perc,
	(p.quote_last_quote * p.shares) - p.amount AS profit_value,
	quote_last_quote_at AS last_update,
	now() - p.quote_last_quote_at as date_diff2,
	justify_interval(now() - p.quote_last_quote_at) as date_diff
FROM securities s INNER JOIN positions p ON s.symbol = p.symbol
WHERE s.category = 'stock' AND p.shares > 0
ORDER BY symbol
--ORDER BY (p.quote_last_quote * p.shares) - p.amount

-- detalles posiciones
--   includes yesterday close value
SELECT
	p.symbol, p.amount, p.quote_purchase, p.quote_last_quote, ticks_yesterday.close,
	(p.quote_last_quote * p.shares) - p.amount AS profit_today_value,
	p.quote_last_quote/p.quote_purchase - 1 AS profits_today_perc,
	p.quote_last_quote/ticks_yesterday.close - 1 AS profits_yesterday_perc
FROM securities s
	INNER JOIN positions p ON s.symbol = p.symbol
	INNER JOIN LATERAL (
	    SELECT close, bucket FROM ticks_1d t1d
	      WHERE p.symbol = t1d.symbol  AND t1d.bucket < CURRENT_DATE
	      ORDER BY bucket DESC LIMIT 1
	  ) AS ticks_yesterday ON true
WHERE s.category = 'stock' AND p.shares > 0
ORDER BY symbol
-- for visual
SELECT
	p.symbol,
	to_char(p.amount, '999990D00') AS amount,
	to_char(p.quote_purchase, '999990D0099') AS quote_quote,
	to_char(p.quote_last_quote, '999990D0099') AS quote_last_quote,
	to_char(ticks_1d_ago.close, '999990D0099') AS ticks_yesterday_close,
	to_char(((p.quote_last_quote * p.shares) - p.amount), '999990D0099') AS profit_today_value,
	to_char((p.quote_last_quote/p.quote_purchase - 1) * 100, '990D99%') AS profits_today_perc,
	to_char((p.quote_last_quote/ticks_1d_ago.close - 1) * 100, '990D99%') AS profits_1d_perc
FROM securities s
	INNER JOIN positions p ON s.symbol = p.symbol
	INNER JOIN LATERAL (
	    SELECT close, bucket FROM ticks_1d t1d
	      WHERE p.symbol = t1d.symbol  AND t1d.bucket < CURRENT_DATE
	      ORDER BY bucket DESC LIMIT 1
	  ) AS ticks_1d_ago ON true
WHERE s.category = 'stock' AND p.shares > 0
ORDER BY (p.quote_last_quote/ticks_1d_ago.close - 1)
(p.quote_last_quote/p.quote_purchase - 1)


-- posiciones con perdidas
SELECT
	p.symbol, p.amount, p.quote_purchase, p.quote_last_quote,
	p.quote_last_quote/p.quote_purchase - 1 AS profits_perc,
	(p.quote_last_quote * p.shares) - p.amount AS profit_value
FROM securities s INNER JOIN positions p ON s.symbol = p.symbol
WHERE s.category = 'stock' AND p.shares > 0 AND (p.quote_last_quote/p.quote_purchase - 1) < 0
ORDER BY (p.quote_last_quote * p.shares) - p.amount

-- total balance
SELECT	
	sum(p.amount) AS investment,
	sum(p.quote_last_quote * p.shares) AS valuation,
	sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 AS profits_perc,
	sum(p.quote_last_quote * p.shares) - sum(p.amount) AS profit_value
FROM securities s INNER JOIN positions p ON s.symbol = p.symbol
WHERE s.category = 'stock' AND p.shares > 0

-- total balance w/yesteday value
SELECT	
	sum(p.amount) AS investment,
	sum(p.quote_last_quote * p.shares) AS valuation,
	sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 AS profits_hist_perc,
	sum(p.quote_last_quote * p.shares) - sum(p.amount) AS profit_hist_value,
	sum(ticks_1d_ago.close * p.shares) / sum(p.amount) - 1 AS profits_1d_ago_perc,
	sum(ticks_1d_ago.close * p.shares) - sum(p.amount) AS profit_1d_ago_value
FROM securities s
	INNER JOIN positions p ON s.symbol = p.symbol
	INNER JOIN LATERAL (
	    SELECT close, bucket FROM ticks_1d t1d
	      WHERE p.symbol = t1d.symbol  AND t1d.bucket < CURRENT_DATE
	      ORDER BY bucket DESC LIMIT 1
	  ) AS ticks_1d_ago ON true
WHERE s.category = 'stock' AND p.shares > 0

-- total perdidas
SELECT
	sum(p.amount) AS investment,
	sum(p.quote_last_quote * p.shares) AS valuation,
	sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 AS profits_perc,
	sum(p.quote_last_quote * p.shares) - sum(p.amount) AS profit_value
FROM securities s INNER JOIN positions p ON s.symbol = p.symbol
WHERE s.category = 'stock' AND p.shares > 0 AND (p.quote_last_quote/p.quote_purchase - 1) < 0

-- total balance by broker
SELECT
	t.broker AS broker,
	sum(p.amount) AS investment,
	sum(p.quote_last_quote * p.shares) AS valuation,
	sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 AS profits_perc,
	sum(p.quote_last_quote * p.shares) - sum(p.amount) AS profit_value
FROM securities s
	INNER JOIN positions p ON s.symbol = p.symbol
	INNER JOIN transactions t ON s.symbol = t.symbol
WHERE s.category = 'stock' AND p.shares > 0
GROUP BY broker

-- assets not updated since last hour
SELECT s.symbol, s.exchange, DATE_TRUNC('hour', now()) - quote_last_quote_at AS datediff
FROM securities s INNER JOIN positions p ON s.symbol = p.symbol
WHERE quote_last_quote_at < DATE_TRUNC('hour', now())


--- SHARPE RATIO (WIP)

SELECT 
	t.symbol, t.bucket,
	t.open, t.high, t.low,
	t.close, returns.close as t1d_close,
	((returns.close/t.close) - 1) * 100  as diff, 
	t.volume,
	returns.ret,
	risk_free.val as risk_free,
	returns.ret - risk_free.val as excess_returns,
	avg(returns.ret - risk_free.val) OVER (PARTITION BY symbol) as mean_returns,
	stddev(returns.ret - risk_free.val) OVER (PARTITION BY symbol) as sd_returns,
	avg(returns.ret - risk_free.val) OVER (PARTITION BY symbol) / stddev(returns.ret - risk_free.val) OVER (PARTITION BY symbol) as sharpe_ratio
FROM ticks_1d t
	JOIN (SELECT 0.05 AS val) as risk_free ON true
	INNER JOIN LATERAL (
  		SELECT
  			t1d.close,
  			(t.close - t1d.close) as close_diff,
  			(t.close - t1d.close) / t1d.close*100 as ret
  		FROM ticks_1d t1d
      	WHERE t.symbol = t1d.symbol AND t1d.bucket = (t.bucket - '1 DAY'::interval)
      	ORDER BY bucket DESC LIMIT 1
	) AS returns ON true
WHERE t.symbol = 'AAOI'