-- SELECT 	pd.*,
-- 			COALESCE(dir, values[array_upper(values, 1)]::int) AS direction,
-- 			CASE
-- 				WHEN LAG(COALESCE(dir, values[array_upper(values, 1)]::int), 1)
-- 					 OVER (PARTITION BY symbol ORDER BY dt DESC) <> dir
-- 				THEN 1 END as is_reset
-- 	FROM (
-- 		SELECT *, array_agg(dir) FILTER (WHERE dir IS NOT NULL) OVER (ORDER BY symbol, dt DESC) AS values
-- 		FROM pivots_dir_flag
-- 	) AS pd
-- 	ORDER BY symbol, dt DESC




SELECT t1d_pivots.*,
	CASE
		WHEN t1d_pivots.hb IS NOT NULL AND t1d_pivots.lb IS NULL THEN 1
		WHEN t1d_pivots.lb IS NOT NULL AND t1d_pivots.hb IS NULL THEN -1
	END AS dir
FROM (
	SELECT t1d.symbol, t1d.dt::date AS dt, t1d.open, t1d.high, t1d.low, t1d.close, t1d.volume,
			CASE 
				WHEN 	MAX(t1d.high) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = t1d.high THEN 'DOWN'
			    WHEN 	MIN(t1d.low) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = t1d.low THEN 'UP'
			ELSE NULL END AS pivot,
			CASE WHEN MAX(high) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = high THEN high ELSE NULL END AS hb,
			CASE WHEN MIN(low) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = low THEN low ELSE NULL END AS lb
	FROM ticks_1d t1d
) AS t1d_pivots 
where symbol = 'AAPL'

-- SELECT t1d.symbol, t1d.dt::date AS dt, t1d.open, t1d.high, t1d.low, t1d.close, t1d.volume,
-- 		CASE 
-- 			WHEN 	MAX(t1d.high) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = t1d.high THEN 'DOWN'
-- 		    WHEN 	MIN(t1d.low) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = t1d.low THEN 'UP'
-- 		ELSE NULL END AS pivot,
-- 		CASE WHEN MAX(high) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = high THEN high ELSE NULL END AS hb,
-- 		CASE WHEN MIN(low) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = low THEN low ELSE NULL END AS lb
-- FROM ticks_1d t1d
-- WHERE t1d.symbol='AAPL'
-- ORDER BY symbol, dt::date DESC


-- SELECT p.*,
-- 		CASE
-- 			WHEN hb IS NOT NULL AND lb IS NULL THEN 1
-- 			WHEN lb IS NOT NULL AND hb IS NULL THEN -1
-- 		END AS dir
-- 	FROM pivots p




WITH ma AS (
	SELECT symbol, dt,
		AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 16 PRECEDING) AS MA_slow,
		AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 64 PRECEDING) AS MA_fast,
		CASE 
			WHEN AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 16 PRECEDING) >
				AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 64 PRECEDING) THEN 1
			WHEN AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 16 PRECEDING) <
				AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 64 PRECEDING) THEN -1
		END MA_dir,
		open, close, low, high, volume
	FROM ticks_4h
	WHERE symbol = 'ADAUSD' AND dt::DATE >= '2024-04-01'
-- 	ORDER BY dt DESC
)
, ma_reversal AS (
	SELECT symbol, dt, open, close, low, high, volume, MA_dir,
		LAG(MA_dir) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC) AS MA_dir_prev,
		CASE
			WHEN MA_dir <> LAG(MA_dir) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC) AND
				MA_dir = -1 
			THEN 1
			WHEN MA_dir <> LAG(MA_dir) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC) AND
				MA_dir = 1 
			THEN -1
		END AS signal, MA_slow, MA_fast
	FROM ma
-- 	ORDER by dt DESC
)
SELECT *
FROM ma_reversal
where dt = '2024-04-11 13:00:00'
order by dt DESC





-- SELECT dt as datetime, open AS Open, close AS Close, high AS High, low AS Low, volume AS Volume
--                           FROM ticks_4h
--                           WHERE symbol ='ADAUSD' AND dt >= '2022-01-01'::DATE
--                           ORDER BY dt



WITH ma AS (
	SELECT symbol, dt,
		AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 16 PRECEDING) AS MA_slow,
		AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 64 PRECEDING) AS MA_fast,
		CASE 
			WHEN AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 16 PRECEDING) >
				AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 64 PRECEDING) THEN 1
			WHEN AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 16 PRECEDING) <
				AVG(close) OVER(PARTITION BY symbol ORDER BY symbol, dt ROWS 64 PRECEDING) THEN -1
		END MA_dir,
		open, close, low, high, volume
	FROM ticks_4h
	WHERE symbol = 'ADAUSD' AND dt::DATE >= '2024-04-01'
-- 	ORDER BY dt DESC
)
, ma_reversal AS (
	SELECT symbol, dt, open, close, low, high, volume, MA_dir,
		LAG(MA_dir) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC) AS MA_dir_prev,
		CASE
			WHEN MA_dir <> LAG(MA_dir) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC) AND
				MA_dir = -1 
			THEN 1
			WHEN MA_dir <> LAG(MA_dir) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC) AND
				MA_dir = 1 
			THEN -1
		END AS signal, MA_slow, MA_fast
	FROM ma
-- 	ORDER by dt DESC
)
SELECT *
FROM ma_reversal
where dt = '2024-04-11 13:00:00'
order by dt DESC





-- SELECT dt as datetime, open AS Open, close AS Close, high AS High, low AS Low, volume AS Volume
--                           FROM ticks_4h
--                           WHERE symbol ='ADAUSD' AND dt >= '2022-01-01'::DATE
--                           ORDER BY dt