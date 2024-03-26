SELECT
	p.symbol, p.amount, p.quote_purchase, p.quote_last_quote, ticks_yesterday.close,
	(p.quote_last_quote * p.shares) - p.amount AS profit_today_value,
	p.quote_last_quote/p.quote_purchase - 1 AS profits_today_perc,
	p.quote_last_quote/ticks_yesterday.close - 1 AS profits_yesterday_perc
FROM assets a
	INNER JOIN positions p ON a.symbol = p.symbol
	INNER JOIN LATERAL (
	    SELECT close, bucket FROM ticks_1d t1d
	      WHERE p.symbol = t1d.symbol  AND t1d.bucket < CURRENT_DATE
	      ORDER BY bucket DESC LIMIT 1
	  ) AS ticks_yesterday ON true
WHERE a.category = 'stock' AND p.shares > 0
ORDER BY symbol