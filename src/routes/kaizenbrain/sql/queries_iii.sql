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