SELECT
	sum(p.amount) AS investment,
	sum(p.quote_last_quote * p.shares) AS valuation,
	sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 	AS profits_hist_perc,
	sum(p.quote_last_quote * p.shares) - sum(p.amount) 			AS profit_hist_value,
	sum(ticks_1d_ago.close * p.shares) / sum(p.amount) - 1 	AS profits_1d_ago_perc,
	sum(ticks_1d_ago.close * p.shares) - sum(p.amount) 			AS profit_1d_ago_value
FROM assets a
	INNER JOIN positions p ON a.symbol = p.symbol
	INNER JOIN LATERAL (
	    SELECT close, dt FROM ticks_1d t1d
	      WHERE p.symbol = t1d.symbol  AND t1d.dt < CURRENT_DATE
	      ORDER BY dt DESC LIMIT 1
	  ) AS ticks_1d_ago ON true
WHERE a.category = 'stock' AND p.shares > 0

-- by portfolio

SELECT
	pa.code,
	sum(p.amount) AS investment,
	sum(p.quote_last_quote * p.shares) AS valuation,
	sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 	AS profits_hist_perc,
	sum(p.quote_last_quote * p.shares) - sum(p.amount) 			AS profit_hist_value,
	sum(ticks_1d_ago.close * p.shares) / sum(p.amount) - 1 	AS profits_1d_ago_perc,
	sum(ticks_1d_ago.close * p.shares) - sum(p.amount) 			AS profit_1d_ago_value
FROM assets a
	INNER JOIN positions p ON a.symbol = p.symbol
	INNER JOIN portfolio_assets pa ON a.symbol = pa.symbol
	INNER JOIN LATERAL (
	    SELECT close, dt FROM ticks_1d t1d
	      WHERE p.symbol = t1d.symbol  AND t1d.dt < CURRENT_DATE
	      ORDER BY dt DESC LIMIT 1
	  ) AS ticks_1d_ago ON true
WHERE a.category = 'stock' AND p.shares > 0
GROUP BY pa.code