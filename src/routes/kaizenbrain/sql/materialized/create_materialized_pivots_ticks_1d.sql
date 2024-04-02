CREATE MATERIALIZED VIEW pivots_ticks_1d AS
WITH pivots AS (
	SELECT 	symbol, dt::date AS dt, high, low, open, close, volume,
			CASE 
		    	WHEN 	MAX(high) 	OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = high THEN 'DOWN'
		    	WHEN 	MIN(low) 	OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = low THEN 'UP'
		    ELSE NULL END AS pivot,
		    max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 11 FOLLOWING) AS next_high,
		    min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 11 FOLLOWING) AS next_low,
		    CASE WHEN max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 11 FOLLOWING) = high THEN high ELSE NULL END AS ph,
		    CASE WHEN min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 11 FOLLOWING) = low THEN low ELSE NULL END AS pl,
		    CASE
		    	WHEN
		    		(CASE WHEN max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 11 FOLLOWING) = high THEN high ELSE NULL END) IS NOT NULL AND
		    		(CASE WHEN min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 11 FOLLOWING) = low THEN low ELSE NULL END) IS NULL
		    	THEN 1
		    	WHEN
		    		(CASE WHEN min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 11 FOLLOWING) = low THEN low ELSE NULL END) IS NOT NULL AND
		    		(CASE WHEN max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 11 FOLLOWING) = high THEN high ELSE NULL END) IS NULL
		    	THEN -1
		    END as dir_flag
	FROM ticks_1d t1d
	ORDER BY symbol, dt::date DESC
)

SELECT 	symbol, dt, high, low, open, close, volume, pivot, dir_flag,
		COALESCE(dir_flag, values[array_upper(values, 1)]::int) AS direction,
		CASE
			WHEN LAG(COALESCE(dir_flag, values[array_upper(values, 1)]::int), 1)
				 OVER (PARTITION BY symbol ORDER BY dt DESC) <> dir_flag
			THEN 1 END as is_reset
FROM (
	SELECT *, array_agg(dir_flag) FILTER (WHERE dir_flag IS NOT NULL) OVER (PARTITION BY symbol ORDER BY symbol, dt DESC) AS values
	FROM pivots
) AS pd
ORDER BY symbol DESC, dt DESC;


CREATE UNIQUE INDEX pivots_ticks_1d_symbol_dt ON pivots_ticks_1d (symbol, dt);

		
-- DROP VIEW levels_longterm
REFRESH MATERIALIZED VIEW pivots_ticks_1d;
DROP MATERIALIZED VIEW pivots_ticks_1d;




-- -- WITH pivots AS (SELECT t1d.symbol,
-- -- 					count(t1d.dt) 	OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS days,
-- -- 					MAX(t1d.high) 	OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS pivot_up,
-- -- 				    MIN(t1d.low) 	OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS pivot_down,
-- -- 				    CASE 
-- -- 				    		WHEN 	MAX(t1d.high) OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = t1d.high AND
-- -- 				    				count(t1d.dt) OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) > 5
-- -- 				    		THEN 	'UP'
-- -- 				    		WHEN 	MIN(t1d.low) OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = t1d.low AND
-- -- 				    				count(t1d.dt) OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) > 5
-- -- 				    		THEN 	'DOWN'
-- -- 				    	ELSE '' END AS pivot
-- -- 				FROM ticks_1d t1d
-- -- 				ORDER BY dt::date DESC)
-- SELECT
-- 	t1d.symbol, t1d.dt::date, t1d.open, t1d.high, t1d.low, t1d.close,
-- 	t1d.volume,
--     p.* 
-- FROM ticks_1d t1d
-- 	JOIN LATERAL (SELECT tt1d.symbol,
-- 						count(tt1d.dt) 	OVER(PARTITION BY symbol ORDER BY tt1d.dt::date DESC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS days,
-- 						MAX(tt1d.high) 	OVER(PARTITION BY symbol ORDER BY tt1d.dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS pivot_up,
-- 					    MIN(tt1d.low) 	OVER(PARTITION BY symbol ORDER BY tt1d.dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS pivot_down,
-- 					    CASE 
-- 					    		WHEN 	MAX(tt1d.high) OVER(PARTITION BY symbol ORDER BY tt1d.dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = tt1d.high AND
-- 					    				count(tt1d.dt) OVER(PARTITION BY symbol ORDER BY tt1d.dt::date DESC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) > 5
-- 					    		THEN 	'UP'
-- 					    		WHEN 	MIN(tt1d.low) OVER(PARTITION BY tt1d.symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = tt1d.low AND
-- 					    				count(tt1d.dt) OVER(PARTITION BY tt1d.symbol ORDER BY dt::date DESC ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) > 5
-- 					    		THEN 	'DOWN'
-- 					    	ELSE '' END AS pivot
-- 				FROM ticks_1d tt1d WHERE t1d.symbol = tt1d.symbol ORDER BY tt1d.dt::date DESC) p ON true
-- -- where symbol = 'CSCO'
-- -- group by symbol
-- order by dt::date desc
-- limit 30