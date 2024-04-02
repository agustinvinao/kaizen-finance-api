------------
-- premarket
------------
INSERT INTO premarket (symbol, tf, occurs_on, note)
VALUES ('WDAY', '1w'::timeframe, DATE_TRUNC('week', now()) + '1 week'::interval, 'retrocedió de 38.2, moviendose entre 2 resistencias, posible toma de ganancias? fase 5?');

INSERT INTO premarket (symbol, tf, occurs_on, note)
VALUES ('T', '1d'::timeframe, DATE_TRUNC('week', now()) + '1 week'::interval, 'acumulando');

SELECT * FROM premarket WHERE symbol = 'AMZN'




-- select min(dt) from daily_ticks where symbol = 'ALB' -- order by dt asc limit 10


-- CREATE MATERIALIZED VIEW levels_annual AS
-- 	WITH levels AS (
-- 		SELECT date_part('year', CURRENT_DATE) as current_year,
-- 				generate_series(-0.9, 1, 0.1) AS level
-- 	)
-- 	SELECT
-- 		a.symbol,
-- 		year_open.year,
-- 		year_open.price as open_price ,
-- 		l.level * 100 AS level,
-- 		year_open.price + l.level * year_open.price AS price
-- 	FROM assets a
-- 		JOIN LATERAL (
-- 			SELECT DATE_PART('year', dt) as year, dt, close AS price
-- 			FROM daily_ticks
-- 			WHERE symbol = 'ALB' AND dt >= DATE_TRUNC('year', now())
-- 			ORDER BY dt ASC LIMIT 1
-- 		) AS year_open ON true
-- 		JOIN levels as l ON true
-- -- 	WHERE a.symbol = 'ALB'
-- 	ORDER BY symbol, l.level;
-- DROP MATERIALIZED VIEW levels_annual;


-- SELECT DATE_PART('year', dt) as year, dt, *
-- 			FROM daily_ticks
-- 			WHERE symbol = 'ALB' AND dt >= DATE_TRUNC('year', now())
-- 			ORDER BY dt ASC LIMIT 1


-- SELECT DATE_PART('year', bucket) as year, bucket, *
-- 			FROM ticks_1d t1d
-- 			WHERE t1d.symbol = 'ALB' AND t1d.bucket >= DATE_TRUNC('year', now())
-- 			ORDER BY t1d.bucket ASC LIMIT 1



-- DROP MATERIALIZED VIEW levels_annual;
-- SELECT * FROM levels_annual WHERE symbol = 'CI' AND soporte = 0.0

SELECT 	p.symbol, p.quote_last_quote,
		la.open_price, la.level, la.price, la.dist
-- 		ll.level, ll.price, ll.dist,
-- 		CASE WHEN t1d.low < la.price AND t1d.high > la.price THEN TRUE
-- 		ELSE FALSE END as crossing,
-- 		t1d.*
FROM positions p
CROSS JOIN LATERAL (
  SELECT la.open_price, la.price, la.level, abs(p.quote_last_quote - la.price) / p.quote_last_quote AS dist
  FROM levels_annual la WHERE p.symbol = la.symbol ORDER BY dist LIMIT 1
) la
-- CROSS JOIN LATERAL (
--   SELECT ll.price, ll.level, abs(p.quote_last_quote - ll.price) / p.quote_last_quote AS dist
--   FROM levels_longterm ll WHERE p.symbol = ll.symbol ORDER BY dist LIMIT 1) ll 
-- CROSS JOIN LATERAL (SELECT * FROM ticks_1d t1d WHERE p.symbol = t1d.symbol ORDER BY bucket DESC LIMIT 1) t1d
-- WHERE (t1d.low < la.price AND t1d.high > la.price) is true
ORDER by p.symbol

-- WHERE p.symbol = 'AMZN'



















WITH min_max AS (
    SELECT tt.symbol, MIN(tt.low) AS low, MAX(tt.high) AS high
	FROM ticks_1d tt WHERE tt.dt::DATE = CURRENT_DATE::DATE
	GROUP BY tt.symbol)
-- open: 15.17, close: 14.1, low: 14.005, high: 15.17
SELECT
	t.symbol,
	FIRST_VALUE(t.open) 	OVER (PARTITION BY t.symbol ORDER BY t.dt ASC) AS open,
	LAST_VALUE(t.close) 	OVER (PARTITION BY t.symbol ORDER BY t.dt ASC) AS close,
	mm.low as low,
	mm.high as high
FROM ticks_1d t JOIN min_max mm ON t.symbol = mm.symbol
WHERE dt::DATE = CURRENT_DATE::DATE AND t.symbol in ('AAOI', 'ON')
ORDER BY dt DESC
LIMIT 2

SELECT a.symbol, a.exchange, DATE_TRUNC('hour', now()) - quote_last_quote_at AS datediff
FROM assets a INNER JOIN positions p ON a.symbol = p.symbol
WHERE quote_last_quote_at < (DATE_TRUNC('hour', now()) - '1 hour'::interval)

-- SELECT * FROM ticks WHERE dt::DATE = CURRENT_DATE::DATE AND symbol = 'AAOI' ORDER BY dt
-- delete from ticks WHERE dt::DATE = CURRENT_DATE::DATE AND symbol in ('AAOI', 'ON')


-- SELECT 
-- 		p.symbol,
-- 		CASE
-- 			WHEN t1d.low < ll.price AND t1d.high > ll.price THEN 'annual'
-- 			WHEN t1d.low < la.price AND t1d.high > la.price THEN 'longterm'
-- 		ELSE '' END as crossing,
-- 		p.quote_last_quote,
-- 		t1d.low, t1d.high, t1d.low, t1d.high,
-- 		la.open_price, la.level, la.price, la.dist,
-- 		ll.level, ll.price, ll.dist
-- FROM positions p
-- 	CROSS JOIN LATERAL (SELECT * FROM daily_ticks t1d WHERE p.symbol = t1d.symbol ORDER BY dt DESC LIMIT 1) t1d
-- CROSS JOIN LATERAL (
--   SELECT la.open_price, la.price, la.level, abs(p.quote_last_quote - la.price) / p.quote_last_quote AS dist
--   FROM levels_annual la WHERE p.symbol = la.symbol ORDER BY dist LIMIT 1
-- ) la
-- 	CROSS JOIN LATERAL (
-- 	  SELECT ll.price, ll.level, abs(p.quote_last_quote - ll.price) / p.quote_last_quote AS dist
-- 	  FROM levels_longterm ll WHERE p.symbol = ll.symbol ORDER BY dist LIMIT 1) ll 
-- WHERE 
-- 	(t1d.low < ll.price AND t1d.high > ll.price) OR
-- 	(t1d.low < la.price AND t1d.high > la.price)
-- ORDER by p.symbol





-- WITH pivots AS (
-- 	SELECT * FROM pivots_ticks_1d WHERE pivot IS NOT NULL ORDER BY dt DESC
-- )
-- SELECT p1d.*
-- -- 	ARRAY[
-- -- 		LEAD(p.dt, 2) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 		LEAD(p.dt, 1) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 		LEAD(p.dt, 0) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC)
-- -- 	] as last_3_dt,
-- -- 	ARRAY[
-- -- 		LEAD(p.pivot, 2) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 		LEAD(p.pivot, 1) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 		LEAD(p.pivot, 0) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC)
-- -- 	] as last_3_pivots,
-- -- 	CASE 
-- -- 		WHEN ARRAY[
-- -- 			LEAD(p.pivot, 2) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 			LEAD(p.pivot, 1) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 			LEAD(p.pivot, 0) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC)
-- -- 		] = ARRAY['UP','DOWN','DOWN'] THEN 'bearish'
-- -- 		WHEN ARRAY[
-- -- 			LEAD(p.pivot, 2) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 			LEAD(p.pivot, 1) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 			LEAD(p.pivot, 0) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC)
-- -- 		] = ARRAY['DOWN','UP','DOWN'] THEN 'bearish'
-- -- 		WHEN ARRAY[
-- -- 			LEAD(p.pivot, 2) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 			LEAD(p.pivot, 1) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 			LEAD(p.pivot, 0) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC)
-- -- 		] = ARRAY['DOWN','DOWN','UP'] THEN 'bearish'
-- -- 		WHEN ARRAY[
-- -- 			LEAD(p.pivot, 2) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 			LEAD(p.pivot, 1) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC),
-- -- 			LEAD(p.pivot, 0) OVER(PARTITION BY p.symbol ORDER BY p.dt::date DESC)
-- -- 		] = ARRAY['DOWN','DOWN','UP'] THEN 'bearish'
-- -- 	ELSE null
-- -- 	END AS phase_direction
-- FROM pivots_ticks_1d p1d JOIN pivots p ON p1d.symbol = p.symbol and p1d.dt = p.dt
-- WHERE p1d.pivot IS NOT NULL AND p1d.symbol = 'PAYC'
-- ORDER BY dt DESC



-- select * from pivots_ticks_1d where symbol = 'PAYC' order by dt desc limit 40
	
-- CREATE MATERIALIZED VIEW pivots_ticks_1d AS
SELECT
	t1d.symbol,
	t1d.dt::date,
	t1d.open,
	t1d.high,
	t1d.low,
	t1d.close,
	t1d.volume,
	MAX(t1d.high) OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) as hilo_hi,
	SUM(1) OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) as hilo_hi_2
-- 	MIN(t1d.low)  OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) as hilo_lo,
-- 	ROW_NUMBER () OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) as hilo_lo_row
-- 	tt.*
FROM ticks_1d t1d
-- 	JOIN LATERAL (
-- 		SELECT 
-- 			MAX(tt1d.high) OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) as hh,
-- 			ROW_NUMBER () OVER(PARTITION BY symbol ORDER BY dt::date DESC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) as hh_row
-- 		FROM ticks_1d tt1d
-- 		WHERE tt1d.symbol = t1d.symbol AND tt1d.dt::date = t1d.dt::date
-- 	) AS tt ON true
WHERE t1d.symbol = 'PAYC' and t1d.dt::date >= '2024-03-01'::date
ORDER BY dt::date DESC


-- drop MATERIALIZED view pivots_ticks_1d




SELECT symbol, grp, max(dt) AS top
FROM pivots_ticks_1d
GROUP BY symbol, grp
ORDER BY symbol, top DESC



-- 	FIRST_VALUE(dt) OVER(PARTITION BY symbol, grp ORDER BY symbol, dt)



WITH pivots AS (
	SELECT t1d.symbol, t1d.dt::date AS dt, t1d.open, t1d.high, t1d.low, t1d.close, t1d.volume,
		    CASE 
		    		WHEN 	MAX(t1d.high) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = t1d.high THEN 'DOWN'
 		    				-- AND count(t1d.dt) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = 23 THEN 'UP'
		    		WHEN 	MIN(t1d.low) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = t1d.low THEN 'UP'
 		    				-- AND count(t1d.dt) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = 23 THEN 'DOWN'
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
), hh_ll AS (
	SELECT 	pd.*,
			CASE 
				WHEN MAX(high) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = high
				THEN high
			ELSE NULL END AS hb,
			CASE 
				WHEN MIN(low) OVER(PARTITION BY symbol ORDER BY dt::DATE DESC ROWS BETWEEN 11 PRECEDING AND 11 FOLLOWING) = low
				THEN low
			ELSE NULL END AS lb
	FROM pivot_dists pd
	ORDER BY symbol, dt DESC, grp)

-- ph and na(pl) ? 1 : pl and na(ph) ? -1 : dir
select hl.*,
	CASE
		WHEN hl.hb IS NOT NULL AND hl.lb IS NULL THEN 1
		WHEN hl.lb IS NOT NULL AND hl.hb IS NULL THEN -1
	END as dir
from hh_ll hl
WHERE symbol IN ('AAPL', 'AAOI')
ORDER BY symbol, dt desc;

-- DROP MATERIALIZED VIEW pivots_ticks_1d;

-- CREATE UNIQUE INDEX pivots_ticks_1d_symbol_dt ON pivots_ticks_1d (symbol, dt);


-- CREATE MATERIALIZED VIEW phases_ticks_1d AS
-- WITH edges AS (
-- 	SELECT symbol, dir, grp, MAX(dt) AS top
-- 	FROM pivots_ticks_1d
-- 	GROUP BY symbol, dir, grp ORDER by symbol, grp)

-- SELECT
-- 	ef.symbol,
-- 	ef.dir,
-- 	ef.grp,
-- 	ef.top as floor,
-- 	lag(ef.top, 1) OVER(PARTITION BY symbol ORDER BY symbol, grp ) as top
-- FROM edges ef
-- WHERE symbol = 'AAPL'

-- -- CREATE INDEX phases_ticks_1d_symbol_dir ON pivots_ticks_1d (symbol, dir)
-- -- DROP MATERIALIZED VIEW phases_ticks_1d;