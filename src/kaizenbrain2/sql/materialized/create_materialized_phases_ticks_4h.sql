CREATE MATERIALIZED VIEW phases_ticks_4h AS
WITH pivots AS (
	SELECT 	symbol, dt AS dt, high, low, open, close, volume,
			CASE 
		    	WHEN 	MAX(high) 	OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = high THEN 'DOWN'
		    	WHEN 	MIN(low) 	OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) = low THEN 'UP'
		    ELSE NULL END AS pivot,
		    CASE
		    	WHEN
		    		(CASE WHEN max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = high THEN high ELSE NULL END) IS NOT NULL
		    		AND (CASE WHEN min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = low THEN low ELSE NULL END) IS NULL
		    	THEN 1
		    	WHEN
		    		(CASE WHEN min(low) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = low THEN low ELSE NULL END) IS NOT NULL
		    		AND (CASE WHEN max(high) OVER(PARTITION BY symbol ORDER BY symbol, dt DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) = high THEN high ELSE NULL END) IS NULL
		    	THEN -1
		    END as dir_flag
	FROM ticks_4h t
)
, pivots_with_dir AS (
	SELECT 	symbol, dt, high, low, open, close, volume, pivot, dir_flag, COALESCE(dir_flag, values[array_upper(values, 1)]::int, 0) AS direction
	FROM (SELECT *, array_agg(dir_flag) FILTER (WHERE dir_flag IS NOT NULL) OVER (PARTITION BY symbol ORDER BY symbol, dt DESC) AS values FROM pivots) AS query
	ORDER BY symbol DESC, dt DESC
)
, pivots_direction AS (
	SELECT	symbol, dt, high, low, open, close, volume, pivot, dir_flag, direction,
			CASE WHEN LAG(direction, 1) OVER (PARTITION BY symbol ORDER BY dt DESC) <> dir_flag THEN 1 END as is_reset
	FROM pivots_with_dir
)
, pivots_grupping AS (
	SELECT *, COUNT(is_reset) OVER (PARTITION BY symbol ORDER BY dt DESC) AS grp 
	FROM pivots_direction
)
, phase_edge AS (
	SELECT symbol, grp, direction, max(dt) AS edge
	FROM pivots_grupping
	GROUP BY symbol, grp, direction
	ORDER BY symbol, edge DESC
)
, phases AS (
	SELECT 	symbol,
		direction,
		ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY grp) AS phase_number,
		LEAD(edge, 1) OVER (PARTITION BY symbol ORDER BY grp) AS starting_on,
		edge as ending_on
	FROM phase_edge
)
, pivots_ticks AS (
	SELECT 	symbol, dt, high, low, open, close, volume, pivot, dir_flag,
			COALESCE(dir_flag, values[array_upper(values, 1)]::int) AS direction,
			CASE WHEN LAG(COALESCE(dir_flag, values[array_upper(values, 1)]::int), 1) OVER (PARTITION BY symbol ORDER BY dt DESC) <> dir_flag THEN 1 END as is_reset
	FROM (SELECT *, array_agg(dir_flag) FILTER (WHERE dir_flag IS NOT NULL) OVER (PARTITION BY symbol ORDER BY symbol, dt DESC) AS values FROM pivots) AS pd
	ORDER BY symbol DESC, dt DESC
)
, phases_data AS (SELECT p.*,
		CASE
			WHEN p.direction = 1 THEN ((abs(pt_starting.low - pt_ending.high)) * 100) / pt_starting.low 
			WHEN p.direction = -1 THEN ((abs(pt_starting.high - pt_ending.low)) * 100) / pt_starting.high 
		END AS delta,
		CASE
			WHEN p.direction = 1 THEN pt_starting.low
			WHEN p.direction = -1 THEN pt_starting.high
		END AS sp_n,
		CASE
			WHEN p.direction = 1 THEN pt_ending.high
			WHEN p.direction = -1 THEN pt_ending.low
		END AS ep_n,
		
		CASE
			WHEN p.direction = 1 THEN pt_ending.high - pt_starting.low
			WHEN p.direction = -1 THEN pt_starting.high - pt_ending.low
		END AS pibs,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) / pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) / pt_ending.low
		END AS pibs_perc,
		CASE
			WHEN p.direction = 1 THEN pt_starting.low
			WHEN p.direction = -1 THEN pt_ending.low
		END AS fib_0,
		CASE
			WHEN p.direction = 1 THEN pt_ending.high
			WHEN p.direction = -1 THEN pt_starting.high
		END AS fib_100,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 0.238 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 0.238 + pt_ending.low
		END AS fib_0238,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 0.382 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 0.382 + pt_ending.low
		END AS fib_0382,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 0.5 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 0.5 + pt_ending.low
		END AS fib_050,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 0.618 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 0.618 + pt_ending.low
		END AS fib_0618,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 0.812 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 0.812 + pt_ending.low
		END AS fib_0812,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 1.25 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 1.25 + pt_ending.low
		END AS fib_125,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 1.5 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 1.5 + pt_ending.low
		END AS fib_150,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 1.618 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 1.618 + pt_ending.low
		END AS fib_1618,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 2 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 2 + pt_ending.low
		END AS fib_200,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 2.618 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 2.618 + pt_ending.low
		END AS fib_2618,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 4.236 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 4.236 + pt_ending.low
		END AS fib_4236,
		CASE
			WHEN p.direction = 1 THEN (pt_ending.high - pt_starting.low) * 5.854 + pt_starting.low
			WHEN p.direction = -1 THEN (pt_starting.high - pt_ending.low) * 5.854 + pt_ending.low
		END AS fib_5854
FROM phases p
	JOIN pivots_ticks pt_starting ON pt_starting.symbol = p.symbol AND pt_starting.dt = p.starting_on
	JOIN pivots_ticks pt_ending ON pt_ending.symbol = p.symbol AND pt_ending.dt = p.ending_on
ORDER BY p.symbol, p.starting_on desc
)
, phases_prices AS (SELECT 
	LAG(delta) OVER (PARTITION BY symbol ORDER BY symbol, phase_number DESC) as delta_prev,
	pd.*
FROM phases_data pd ORDER BY phase_number
)
, phases_prices_all AS (SELECT 
	lead(sp_n, 2) OVER (PARTITION BY symbol ORDER BY symbol, phase_number) AS sp_n_1,
	lead(sp_n, 4) OVER (PARTITION BY symbol ORDER BY symbol, phase_number) AS sp_n_2,
	lead(ep_n, 2) OVER (PARTITION BY symbol ORDER BY symbol, phase_number) AS ep_n_1,
	lead(ep_n, 4) OVER (PARTITION BY symbol ORDER BY symbol, phase_number) AS ep_n_2,
	pp.*
FROM phases_prices pp order by symbol, phase_number)
SELECT 
	CASE
		WHEN direction = 1 AND sp_n_2 > sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 < ep_n THEN 1
		WHEN direction = 1 AND sp_n_2 > sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 > ep_n THEN -1
		WHEN direction = 1 AND sp_n_2 < sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 < ep_n THEN -1 -- -2
		WHEN direction = 1 AND sp_n_2 < sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -3
		WHEN direction = 1 AND  sp_n_2 < sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 > ep_n THEN 1 -- 2
		WHEN direction = 1 AND sp_n_2 > sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 < ep_n THEN 1 -- 3
		WHEN direction = 1 AND sp_n_2 < sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -4
		WHEN direction = 1 AND sp_n_2 > sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 < ep_n THEN 1 -- 4
		WHEN direction = 1 AND sp_n_2 < sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 < ep_n THEN 1 -- 5
		WHEN direction = 1 AND sp_n_2 < sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 < ep_n THEN 1 -- 6
		WHEN direction = 1 AND sp_n_2 < sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -5
		WHEN direction = 1 AND sp_n_2 > sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -6
		WHEN direction = 1 AND sp_n_2 > sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -7
		WHEN direction = 1 AND sp_n_2 > sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -8
		WHEN direction = 1 AND sp_n_2 < sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 < ep_n THEN 1 -- 7
		WHEN direction = -1 AND sp_n_2 < sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 > ep_n THEN -1
		WHEN direction = -1 AND sp_n_2 > sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -2
		WHEN direction = -1 AND sp_n_2 > sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -3
		WHEN direction = -1 AND sp_n_2 < sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 < ep_n THEN -1 -- -4
		WHEN direction = -1 AND sp_n_2 > sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 < ep_n THEN 1 -- 1
		WHEN direction = -1 AND sp_n_2 < sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 < ep_n THEN -1 -- -5
		WHEN direction = -1 AND sp_n_2 < sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -6
		WHEN direction = -1 AND sp_n_2 < sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 < ep_n THEN 1 -- 2
		WHEN direction = -1 AND sp_n_2 < sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 < ep_n THEN 1 -- 3
		WHEN direction = -1 AND sp_n_2 > sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 < ep_n THEN 1 -- 4
		WHEN direction = -1 AND sp_n_2 > sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -7
		WHEN direction = -1 AND sp_n_2 > sp_n_1 AND sp_n_1 < sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -8
		WHEN direction = -1 AND sp_n_2 > sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 > ep_n_1 AND ep_n_1 < ep_n THEN -1 -- -9
		WHEN direction = -1 AND sp_n_2 < sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 > ep_n THEN -1 -- -10
		WHEN direction = -1 AND sp_n_2 > sp_n_1 AND sp_n_1 > sp_n AND ep_n_2 < ep_n_1 AND ep_n_1 < ep_n THEN -1 -- -11
	END as trend, 
	*
FROM phases_prices_all

CREATE INDEX phases_ticks_4h_symbol_phase_number ON phases_ticks_4h (symbol, phase_number);

		
REFRESH MATERIALIZED VIEW phases_ticks_4h;
DROP MATERIALIZED VIEW phases_ticks_4h;