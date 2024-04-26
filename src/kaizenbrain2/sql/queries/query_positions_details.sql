SELECT
p.created_at,
	p.symbol, p.amount, p.quote_purchase, p.quote_last_quote,
	p.quote_last_quote/p.quote_purchase - 1 AS profits_perc,
	(p.quote_last_quote * p.shares) - p.amount AS profit_value,
	quote_last_quote_at AS last_update,
	now() - p.quote_last_quote_at as date_diff2,
	justify_interval(now() - p.quote_last_quote_at) as date_diff
FROM assets a INNER JOIN positions p ON a.symbol = p.symbol
WHERE a.category = 'stock' AND p.shares > 0
ORDER BY symbol
--ORDER BY (p.quote_last_quote * p.shares) - p.amount