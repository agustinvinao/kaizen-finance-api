SELECT
	t.broker AS broker,
	sum(p.amount) AS investment,
	sum(p.quote_last_quote * p.shares) AS valuation,
	sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 AS profits_perc,
	sum(p.quote_last_quote * p.shares) - sum(p.amount) AS profit_value
FROM securities s
	INNER JOIN positions p ON s.symbol = p.symbol
	INNER JOIN transactions t ON s.symbol = t.symbol
WHERE s.category = 'stock' AND p.shares > 0
GROUP BY broker
