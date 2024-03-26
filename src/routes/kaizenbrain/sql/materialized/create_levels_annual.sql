CREATE MATERIALIZED VIEW levels_annual AS
	WITH soportes AS (
		SELECT date_part('year', CURRENT_DATE) as current_year,
				generate_series(-0.9, 1, 0.1) AS soporte
	)
	SELECT
		a.symbol,
		year_open.year,
		year_open.open,
		s.soporte * 100 AS soporte,
		year_open.open + s.soporte * year_open.open AS price
	FROM assets a
		JOIN LATERAL (
			SELECT DATE_PART('year', bucket) as year, bucket, open
			FROM ticks_1d t1d
			WHERE t1d.symbol = a.symbol AND t1d.bucket >= DATE_TRUNC('year', now())
			ORDER BY t1d.bucket ASC LIMIT 1
		) AS year_open ON true
		JOIN soportes as s ON true
	ORDER BY symbol, s.soporte;

-- DROP view levels_annual

REFRESH MATERIALIZED VIEW levels_annual;