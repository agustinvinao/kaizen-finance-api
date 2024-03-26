-- lista acciones que cierran con valor menor al valor de la hora anterior
SELECT * FROM (
	SEELCT symbol, dt, close,
		LEAD(close, 1) over (PARTITION BY symbol ORDER BY dt DESC) AS previous_close
	from ticks
) a
WHERE close < previous_close AND extract(HOUR FROM dt) = 20 AND extract(YEAR FROM dt) = 2024
ORDER BY dt DESC