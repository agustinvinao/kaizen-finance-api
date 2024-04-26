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
	FROM ticks_1d where extract(year from bucket) = 2024
) a
WHERE
	close > previous_previous_close AND
	previous_close < previous_previous_close AND
	previous_previous_close < previous_previous_previous_close AND
	volume > previous_volume AND
	previous_close < previous_previous_close AND
	previous_previous_close < previous_previous_previous_close