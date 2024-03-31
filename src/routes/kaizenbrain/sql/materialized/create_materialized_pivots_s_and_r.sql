SELECT
	symbol,
	dt::DATE,
	open,
	high,
	low,
	close,
	(lead(high, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
	lead(low, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
	lead(close, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc)) / 3 AS pivot,
		
	((
		lead(high, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
		lead(low, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
		lead(close, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc)
	)  / 3) * 2 - lead(high, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) AS s1,
	
	((
		lead(high, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
		lead(low, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
		lead(close, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc)
	)  / 3) - (
		lead(high, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) -
		lead(low, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc)
	) AS s2,
	
	((
		lead(high, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
		lead(low, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
		lead(close, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc)
	)  / 3) * 2 - lead(low, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) AS r1,
	
	((
		lead(high, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
		lead(low, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) +
		lead(close, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc)
	)  / 3) + (
		lead(high, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc) -
		lead(low, 1) over(PARTITION BY symbol ORDER BY dt::DATE desc)
	) AS r2
	
FROM ticks_1d
WHERE symbol = 'PAYC'
ORDER BY dt DESC
LIMIT 10