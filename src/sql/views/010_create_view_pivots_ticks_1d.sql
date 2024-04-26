CREATE VIEW pivots_ticks_1d AS
WITH pivots AS (
	SELECT 	symbol, dt AS dt, high, low, open, close, volume,
		    CASE
		    	WHEN
		    		(CASE WHEN max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = high THEN high ELSE NULL END) IS NOT NULL
		    		AND (CASE WHEN min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = low THEN low ELSE NULL END) IS NULL
		    	THEN 1
		    	WHEN
		    		(CASE WHEN min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = low THEN low ELSE NULL END) IS NOT NULL
		    		AND (CASE WHEN max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = high THEN high ELSE NULL END) IS NULL
		    	THEN -1
		    END as dir_flag,
			CASE 
		    	WHEN 	MAX(high) 	OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = high THEN -1
		    	WHEN 	MIN(low) 	OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = low THEN 1
		    ELSE NULL END AS pivot
	FROM ticks_1d t
-- 	WHERE t.symbol='PYPL' AND dt::DATE >= '2023-01-01'
	ORDER BY dt DESC
)
, pivots_confirmation AS (
	SELECT 	p.symbol, p.dt, p.high, p.low, p.open, p.close, p.volume, p.pivot,
			CASE
				WHEN (p.pivot = 1 OR p.pivot = -1 )
					 AND count(p.dt) OVER(PARTITION BY p.symbol ORDER BY p.symbol, p.dt ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) >= 5
				THEN 1
				WHEN (p.pivot = 1 OR p.pivot = -1 )
					 AND count(p.dt) OVER(PARTITION BY p.symbol ORDER BY p.symbol, p.dt ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) < 5
				THEN 0
			END AS pivot_confirmed
	FROM pivots p
)
, pivots_rows AS (
	SELECT *
	FROM pivots_confirmation
	WHERE pivot IS NOT NULL
)
, pivots_direction AS (
	SELECT 	pr.symbol, pr.dt, pr.high, pr.low, pr.open, pr.close, pr.volume, pr.pivot, pr.pivot_confirmed,
			CASE WHEN pr.pivot = 1 THEN pr.low WHEN pr.pivot = -1 THEN pr.high END AS pivot_value,
			LEAD(CASE WHEN pr.pivot = 1 THEN pr.low WHEN pr.pivot = -1 THEN pr.high END) OVER (PARTITION BY pr.symbol ORDER BY pr.dt DESC) pivot_value_prev,
			LEAD(CASE WHEN pr.pivot = 1 THEN pr.low WHEN pr.pivot = -1 THEN high END) OVER (PARTITION BY pr.symbol, pivot ORDER BY pr.dt DESC) pivot_value_prev_same_dir,
			CASE
				WHEN (CASE WHEN pivot = 1 THEN pr.low WHEN pr.pivot = -1 THEN pr.high END) > LEAD(CASE WHEN pr.pivot = 1 THEN pr.low WHEN pr.pivot = -1 THEN pr.high END) OVER (PARTITION BY pr.symbol, pr.pivot ORDER BY dt DESC) 
				THEN 1 ELSE -1
			END AS pivot_dir
	FROM pivots_rows pr
	ORDER BY pr.dt
)
SELECT 	pd.symbol, pd.dt, pd.high, pd.low, pd.open, pd.close, pd.volume, pd.pivot, 
		pd.pivot_confirmed, pd.pivot_value, pd.pivot_value_prev, pd.pivot_value_prev_same_dir, pd.pivot_dir,
		pivot_dir <> LAG(pivot_dir) OVER(PARTITION BY symbol ORDER BY dt DESC) AS pivot_is_reverse
FROM pivots_direction pd
ORDER BY dt


REFRESH MATERIALIZED VIEW pivots_ticks_1d;
DROP MATERIALIZED VIEW pivots_ticks_1d;