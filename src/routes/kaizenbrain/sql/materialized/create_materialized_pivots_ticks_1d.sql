CREATE MATERIALIZED VIEW pivots_ticks_1d AS
WITH pivots AS (
	SELECT t1d.symbol, t1d.dt::date AS dt, t1d.open, t1d.high, t1d.low, t1d.close, t1d.volume,
		    CASE 
		    		WHEN 	MAX(t1d.high) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) = t1d.high AND
		    				count(t1d.dt) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) = 21				THEN 'UP'
		    		WHEN 	MIN(t1d.low) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) = t1d.low AND
		    				count(t1d.dt) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) = 21				THEN 'DOWN'
		    	ELSE NULL END AS pivot
	FROM ticks_1d t1d ORDER BY symbol, dt::date DESC
), pivots_grps AS (
	SELECT p.*, SUM(CASE WHEN p.pivot = 'UP' OR p.pivot = 'DOWN' THEN 1 ELSE 0 END) over (PARTITION BY p.symbol ORDER BY p.dt DESC) AS grp
	FROM ticks_1d tt1d INNER JOIN pivots p ON tt1d.symbol = p.symbol AND tt1d.dt::DATE = p.dt
), pivots_grps_count AS (
	SELECT p.symbol, pg.grp, count(p.dt) as cnt FROM pivots p INNER JOIN pivots_grps pg ON p.symbol = pg.symbol AND p.dt = pg.dt
	GROUP BY p.symbol, pg.grp ORDER BY p.symbol
), pivot_dists as (
	SELECT 	pg.*,
			CASE WHEN pivot IS NULL THEN pgc.cnt - (row_number() over (PARTITION BY pg.symbol, pg.grp ORDER BY pg.symbol, pg.dt DESC) - 1)
			ELSE 0 END AS pivot_dist
	FROM pivots_grps pg JOIN pivots_grps_count pgc ON pg.symbol = pgc.symbol AND pg.grp = pgc.grp
	ORDER BY symbol, dt DESC, grp
)

SELECT 	pd.*,
		FIRST_VALUE(pivot) over (partition by symbol, grp order by symbol, dt DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dir,
		min(dt) over (partition by symbol, grp order by symbol, dt DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dt_min
FROM pivot_dists pd ORDER BY symbol, dt DESC, grp;


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