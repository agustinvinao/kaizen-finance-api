CREATE MATERIALIZED VIEW pivots_ticks_4h AS

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
	FROM ticks_4h t
-- 	WHERE t.symbol='ADAUSD' AND dt::DATE >= '2024-03-10'
	ORDER BY dt DESC
)
, pivots_groups AS (
	SELECT 
		*,
		CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high END AS pivot_value,
		LEAD(CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high END) OVER (PARTITION BY symbol, pivot ORDER BY dt DESC) pivot_value_prev,
		CASE
			WHEN pivot = 1 AND
				 CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high
				 END < LEAD(
				 	CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high END
				 ) OVER (
				 	PARTITION BY symbol, pivot ORDER BY dt DESC
				 )
			THEN -1
			WHEN pivot = 1 AND
				 CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high
				 END > LEAD(
				 	CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high END
				 ) OVER (
				 	PARTITION BY symbol, pivot ORDER BY dt DESC
				 )
			THEN 1
		END AS pivot_dir_up,
		CASE
			WHEN pivot = -1 AND
				 CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high
				 END < LEAD(
				 	CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high END
				 ) OVER (
				 	PARTITION BY symbol, pivot ORDER BY dt DESC
				 )
			THEN -1
			WHEN pivot = -1 AND
				 CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high
				 END > LEAD(
				 	CASE WHEN pivot = 1 THEN low WHEN pivot = -1 THEN high END
				 ) OVER (
				 	PARTITION BY symbol, pivot ORDER BY dt DESC
				 )
			THEN 1
		END AS pivot_dir_down
	FROM pivots
	WHERE pivot IS NOT NULL
	ORDER BY dt DESC
)
SELECT *,
	pivot_dir_up <> LAG(pivot_dir_up) OVER(PARTITION BY symbol, pivot ORDER BY dt DESC) AS pivot_is_reverse_up,
	pivot_dir_down <> LAG(pivot_dir_down) OVER(PARTITION BY symbol, pivot ORDER BY dt DESC) AS pivot_is_reverse_down
FROM pivots_groups
ORDER BY dt DESC

REFRESH MATERIALIZED VIEW pivots_ticks_4h;
DROP MATERIALIZED VIEW pivots_ticks_4h;