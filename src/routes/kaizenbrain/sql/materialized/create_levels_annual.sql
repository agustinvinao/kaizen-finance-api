CREATE MATERIALIZED VIEW levels_annual AS
WITH levels AS (
		SELECT date_part('year', CURRENT_DATE) as current_year,
				generate_series(-0.9, 1, 0.1) AS level
	)
SELECT
	a.symbol,
	year_open.year,
	year_open.price as open_price ,
	l.level * 100 AS level,
	year_open.price + l.level * year_open.price AS price
FROM assets a
	JOIN LATERAL (
		SELECT symbol, DATE_PART('year', dt) as year, dt, open AS price
		FROM daily_ticks t1d
		WHERE t1d.dt >= DATE_TRUNC('year', now()) AND t1d.symbol = a.symbol
		ORDER BY t1d.dt ASC LIMIT 1
	) AS year_open ON true
	JOIN levels as l ON true
ORDER BY symbol, l.level;


-- DROP view levels_annual

REFRESH MATERIALIZED VIEW levels_annual;

DROP MATERIALIZED VIEW levels_annual;