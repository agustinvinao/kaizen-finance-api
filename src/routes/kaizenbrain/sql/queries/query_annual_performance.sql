SELECT 
	p.symbol,
	p.quote_purchase,
	year_open.open AS beginning_of_year_open,
	t.close AS last_close,
	(t.close / year_open.open - 1) * 100  AS year_performance,
	(t.close / p.quote_purchase - 1) * 100 AS porfolio_performance,
	p.created_at
FROM positions p
	JOIN LATERAL (
		SELECT close AS close FROM ticks_1d WHERE symbol = p.symbol
		ORDER BY dt DESC LIMIT 1
	) AS t ON true
	JOIN LATERAL (
		SELECT t1d.open AS open FROM ticks_1d t1d
		WHERE t1d.symbol = p.symbol AND t1d.dt >= DATE_TRUNC('year', now())
		ORDER BY t1d.dt ASC LIMIT 1
	) AS year_open ON true
WHERE p.shares > 0
ORDER BY (t.close / year_open.open - 1) * 100