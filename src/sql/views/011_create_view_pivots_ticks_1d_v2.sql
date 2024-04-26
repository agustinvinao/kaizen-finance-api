CREATE VIEW ticks_on_steroids_1d AS

WITH indicators AS (
    SELECT 
        dt, symbol, open, close, high, low, volume,
        AVG(close) OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 3 		PRECEDING AND CURRENT ROW) AS sma_fast,
        AVG(close) OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 15 	PRECEDING AND CURRENT ROW) AS sma_slow,
        GREATEST(
	    		high - low,
	    		ABS(high - LAG(close) 	OVER (PARTITION BY symbol ORDER BY dt)),
	    		ABS(low - LAG(close) 	OVER (PARTITION BY symbol ORDER BY dt))
	    	) AS true_range,
        CASE 
            WHEN high > LAG(high, 1) 	OVER (PARTITION BY symbol ORDER BY dt) AND
                 high > LAG(high, 2) 	OVER (PARTITION BY symbol ORDER BY dt) AND
                 high > LAG(high, 3) 	OVER (PARTITION BY symbol ORDER BY dt) AND
                 high > LAG(high, 4) 	OVER (PARTITION BY symbol ORDER BY dt) AND
                 high > LAG(high, 5) 	OVER (PARTITION BY symbol ORDER BY dt) AND
                 high > LEAD(high, 1) OVER (PARTITION BY symbol ORDER BY dt) AND
                 high > LEAD(high, 2) OVER (PARTITION BY symbol ORDER BY dt) AND
                 high > LEAD(high, 3) OVER (PARTITION BY symbol ORDER BY dt) AND
                 high > LEAD(high, 4) OVER (PARTITION BY symbol ORDER BY dt) AND
                 high > LEAD(high, 5) OVER (PARTITION BY symbol ORDER BY dt) THEN 1  -- Pivot Up
            WHEN low < LAG(low, 1) 		OVER (PARTITION BY symbol ORDER BY dt) AND
                 low < LAG(low, 2) 		OVER (PARTITION BY symbol ORDER BY dt) AND
                 low < LAG(low, 3) 		OVER (PARTITION BY symbol ORDER BY dt) AND 
                 low < LAG(low, 4) 		OVER (PARTITION BY symbol ORDER BY dt) AND
                 low < LAG(low, 5) 		OVER (PARTITION BY symbol ORDER BY dt) AND
                 low < LEAD(low, 1) 	OVER (PARTITION BY symbol ORDER BY dt) AND
                 low < LEAD(low, 2) 	OVER (PARTITION BY symbol ORDER BY dt) AND
                 low < LEAD(low, 3) 	OVER (PARTITION BY symbol ORDER BY dt) AND
                 low < LEAD(low, 4) 	OVER (PARTITION BY symbol ORDER BY dt) AND
                 low < LEAD(low, 5) 	OVER (PARTITION BY symbol ORDER BY dt) THEN -1 -- Pivot Down
        END AS pivot
    FROM ticks_1d
    -- WHERE symbol = 'PYPL'
)
, pivot_types_crossing AS (
    SELECT 
        dt, symbol, open, close, high, low, volume,
        pivot,
        sma_fast, sma_slow,
        true_range,
	    AVG(true_range) OVER (PARTITION BY symbol ORDER BY dt) AS atr,
	    AVG(true_range) OVER (PARTITION BY symbol ORDER BY dt ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS atr_window,
        CASE
        		WHEN sma_fast > sma_slow AND LAG(sma_fast) OVER (PARTITION BY symbol ORDER BY dt) < LAG(sma_slow) OVER (PARTITION BY symbol ORDER BY dt) THEN -1
        		WHEN sma_fast < sma_slow AND LAG(sma_fast) OVER (PARTITION BY symbol ORDER BY dt) > LAG(sma_slow) OVER (PARTITION BY symbol ORDER BY dt) THEN 1
        	END AS sma_crossing,
        	COUNT(pivot) OVER (PARTITION BY symbol ORDER BY dt) as pivot_grp
    FROM indicators
),
directions AS (
	SELECT 
			t.dt, t.symbol, t.open, t.close, t.high, t.low, t.volume,
	        pc.sma_fast, pc.sma_slow, pc.sma_crossing,
			pc.true_range, pc.atr, pc.atr_window,
		    pc.pivot, pivot_grp, FIRST_VALUE(pc.pivot) over (partition by pc.pivot_grp order by t.dt) as dir
	FROM ticks_1d t LEFT JOIN pivot_types_crossing pc ON t.symbol = pc.symbol AND t.dt = pc.dt
)
SELECT 	dt, symbol, open, close, high, low, volume,
        sma_fast, sma_slow, sma_crossing,
		true_range, atr, atr_window, dir,
		CASE WHEN dir <> LAG(dir) OVER (PARTITION BY symbol ORDER BY dt) THEN 1 END AS dir_change
FROM directions

-- where t.symbol = 'PYPL'
-- ORDER BY t.symbol, t.dt desc
-- ORDER BY t.dt DESC

REFRESH VIEW ticks_on_steroids_1d;
DROP VIEW ticks_on_steroids_1d;