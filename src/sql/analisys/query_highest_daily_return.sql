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
FROM daily_factor JOIN assets ON assets.symbol = daily_factor.symbol
GROUP BY bucket
ORDER BY bucket DESC, max_daily_factor DESC
