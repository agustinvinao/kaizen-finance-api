SELECT	
	sum(p.amount) AS investment,
	sum(p.quote_last_quote * p.shares) AS valuation,
	sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 AS profits_hist_perc,
	sum(p.quote_last_quote * p.shares) - sum(p.amount) AS profit_hist_value,
	sum(ticks_1d_ago.close * p.shares) / sum(p.amount) - 1 AS profits_1d_ago_perc,
	sum(ticks_1d_ago.close * p.shares) - sum(p.amount) AS profit_1d_ago_value
FROM securities s
	INNER JOIN positions p ON s.symbol = p.symbol
	INNER JOIN LATERAL (
	    SELECT close, bucket FROM ticks_1d t1d
	      WHERE p.symbol = t1d.symbol  AND t1d.bucket < CURRENT_DATE
	      ORDER BY bucket DESC LIMIT 1
	  ) AS ticks_1d_ago ON true
WHERE s.category = 'stock' AND p.shares > 0
