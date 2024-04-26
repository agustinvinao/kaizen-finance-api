WITH ma AS (
	SELECT symbol, dt,
		AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 16 PRECEDING) AS MA_slow,
		AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 64 PRECEDING) AS MA_high,
		CASE 
			WHEN AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 16 PRECEDING) >
				AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 64 PRECEDING) THEN 1
			WHEN AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 16 PRECEDING) <
				AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 64 PRECEDING) THEN -1
		END MA_dir
	FROM ticks_4h
	WHERE symbol = 'ADAUSD' AND dt::DATE >= '2024-01-01'
	ORDER BY dt DESC
)
, ma_reversal AS (
	SELECT symbol, dt, MA_dir,
		LAG(MA_dir) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC) AS MA_dir_prev,
		MA_dir <> LAG(MA_dir) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC) AS MA_is_reversal
	FROM ma
	ORDER by dt DESC
)
, pivot_dir_groups AS (
	SELECT 
		mar.symbol,
		mar.dt,
		MA_dir = 1 AS signal_I,
		COUNT(pivot_dir_down) OVER (PARTITION BY mar.symbol ORDER BY mar.symbol, mar.dt DESC) AS pivot_dir_down_grp,
		COUNT(pivot_dir_up) OVER (PARTITION BY mar.symbol ORDER BY mar.symbol, mar.dt DESC) AS pivot_dir_up_grp,
		COUNT(pivot_value) OVER (PARTITION BY mar.symbol ORDER BY mar.symbol, mar.dt DESC) AS pivot_value_grp,
		MA_dir,
		ma_dir_prev,
		pivot_dir_up,
		pivot_value,
		pivot
	FROM ma_reversal mar LEFT JOIN pivots_ticks_4h pt ON mar.symbol = pt.symbol AND mar.dt = pt.dt
	WHERE mar.symbol = 'ADAUSD' AND mar.dt::DATE >= '2024-01-01'
	ORDER by mar.dt DESC
)
, pivots_fill AS (
SELECT 
	symbol, dt, signal_I,
	pivot_dir_down_grp, pivot_dir_up_grp,
	ma_dir, ma_dir_prev,
	pivot_dir_up, pivot_value, pivot, pivot_value_grp,
	FIRST_VALUE(pivot_value) over (partition by symbol, pivot_value_grp order by dt desc) AS pivot_value_fill
FROM pivot_dir_groups pdg
ORDER by dt DESC
)
select 
	symbol, dt,
	signal_I,
	CASE
		WHEN signal_I IS true AND ma_dir = 1 AND ma_dir_prev = -1
		THEN (
			SELECT pivot_value FROM pivots_ticks_4h pt WHERE pt.dt < pf.dt AND pt.pivot = 1 limit 1
		)
	END AS level_0,
	CASE
		WHEN signal_I IS true AND ma_dir = 1 AND ma_dir_prev = -1
		THEN (
			SELECT pivot_value FROM pivots_ticks_4h pt WHERE pt.dt < pf.dt AND pt.pivot = -1 limit 1
		)
	END AS level_100,
	pivot_value_fill,
	pivot_value_grp,
	pivot_dir_up, pivot_value, pivot
-- 	*
from pivots_fill pf
ORDER by dt DESC


-- select * from pivots_ticks_4h where dt < '2024-04-11 13:00:00+00' AND pivot = 1 limit 1