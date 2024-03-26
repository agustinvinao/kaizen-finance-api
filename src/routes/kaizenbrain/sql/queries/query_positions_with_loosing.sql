SELECT
	p.symbol, p.amount, p.quote_purchase, p.quote_last_quote,
	p.quote_last_quote/p.quote_purchase - 1 AS profits_perc,
	(p.quote_last_quote * p.shares) - p.amount AS profit_value
FROM securities s INNER JOIN positions p ON s.symbol = p.symbol
WHERE s.category = 'stock' AND p.shares > 0 AND (p.quote_last_quote/p.quote_purchase - 1) < 0
ORDER BY (p.quote_last_quote * p.shares) - p.amount