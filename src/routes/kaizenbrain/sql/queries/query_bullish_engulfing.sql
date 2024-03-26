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