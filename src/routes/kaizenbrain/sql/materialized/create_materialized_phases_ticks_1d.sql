CREATE MATERIALIZED VIEW phases_ticks_1d AS
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
		    		(CASE WHEN max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = high THEN high ELSE NULL END) IS NOT NULL
		    		AND (CASE WHEN min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = low THEN low ELSE NULL END) IS NULL
		    	THEN 1
		    	WHEN
		    		(CASE WHEN min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = low THEN low ELSE NULL END) IS NOT NULL
		    		AND (CASE WHEN max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = high THEN high ELSE NULL END) IS NULL
		    		-- AND count(dt) 	OVER(PARTITION BY symbol ORDER BY symbol, dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 1 FOLLOWING) > 10
		    	THEN -1
		    END as dir_flag
	FROM ticks_1d t1d
	ORDER BY symbol, dt::date DESC
), pivots_with_dir AS (
	SELECT 	symbol, dt, high, low, open, close, volume, pivot, dir_flag, COALESCE(dir_flag, values[array_upper(values, 1)]::int, 0) AS direction
	FROM (SELECT *, array_agg(dir_flag) FILTER (WHERE dir_flag IS NOT NULL) OVER (PARTITION BY symbol ORDER BY symbol, dt DESC) AS values FROM pivots) AS query
	ORDER BY symbol DESC, dt DESC
), pivots_direction AS (
	SELECT	symbol, dt, high, low, open, close, volume, pivot, dir_flag, direction,
			CASE WHEN LAG(direction, 1) OVER (PARTITION BY symbol ORDER BY dt DESC) <> dir_flag THEN 1 END as is_reset
	FROM pivots_with_dir
), pivots_grupping AS (
	SELECT *, COUNT(is_reset) OVER (PARTITION BY symbol ORDER BY dt DESC) AS grp 
	FROM pivots_direction
), phase_edge AS (
	SELECT symbol, grp, direction, max(dt) AS edge
	FROM pivots_grupping
	GROUP BY symbol, grp, direction
	ORDER BY symbol, edge DESC
), phases AS (
	SELECT 	symbol,
		direction,
		ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY grp) AS phase_number,
		LEAD(edge, 1) OVER (PARTITION BY symbol ORDER BY grp) AS starting_on,
		edge as ending_on
	FROM phase_edge
)
SELECT 	p.*,
		CASE
			WHEN p.direction = 1 THEN pt1d_ending.high - pt1d_starting.low
			WHEN p.direction = -1 THEN pt1d_starting.high - pt1d_ending.low
		END AS pibs,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) / pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) / pt1d_ending.low
		END AS pibs_perc,
		CASE
			WHEN p.direction = 1 THEN pt1d_starting.low
			WHEN p.direction = -1 THEN pt1d_ending.low
		END AS fib_0,
		CASE
			WHEN p.direction = 1 THEN pt1d_ending.high
			WHEN p.direction = -1 THEN pt1d_starting.high
		END AS fib_100,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 0.238 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 0.238 + pt1d_ending.low
		END AS fib_238,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 0.382 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 0.382 + pt1d_ending.low
		END AS fib_382,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 0.5 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 0.5 + pt1d_ending.low
		END AS fib_50,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 0.618 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 0.618 + pt1d_ending.low
		END AS fib_618,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 0.812 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 0.812 + pt1d_ending.low
		END AS fib_812,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 1.25 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 1.25 + pt1d_ending.low
		END AS fib_125,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 1.5 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 1.5 + pt1d_ending.low
		END AS fib_150,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 1.618 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 1.618 + pt1d_ending.low
		END AS fib_1618,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 2 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 2 + pt1d_ending.low
		END AS fib_200,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 2.618 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 2.618 + pt1d_ending.low
		END AS fib_2618,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 4.236 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 4.236 + pt1d_ending.low
		END AS fib_4236,
		CASE
			WHEN p.direction = 1 THEN (pt1d_ending.high - pt1d_starting.low) * 5.854 + pt1d_starting.low
			WHEN p.direction = -1 THEN (pt1d_starting.high - pt1d_ending.low) * 5.854 + pt1d_ending.low
		END AS fib_5854
FROM phases p
	JOIN pivots_ticks_1d pt1d_starting ON pt1d_starting.symbol = p.symbol AND pt1d_starting.dt::date = p.starting_on
	JOIN pivots_ticks_1d pt1d_ending ON pt1d_ending.symbol = p.symbol AND pt1d_ending.dt::date = p.ending_on
	
-- WHERE p.symbol = 'AAOI'
ORDER BY p.symbol, p.starting_on desc;

CREATE INDEX phases_ticks_1d_symbol_dir ON pivots_ticks_1d (symbol, phase_number);

		
REFRESH MATERIALIZED VIEW phases_ticks_1d;
DROP MATERIALIZED VIEW phases_ticks_1d;