SELECT	
	sum(p.amount) AS investment,
	sum(p.quote_last_quote * p.shares) AS valuation,
	sum(p.quote_last_quote * p.shares) / sum(p.amount) - 1 AS profits_perc,
	sum(p.quote_last_quote * p.shares) - sum(p.amount) AS profit_value
FROM assets a INNER JOIN positions p ON a.symbol = p.symbol
WHERE a.category = 'stock' AND p.shares > 0
-- AND (p.quote_last_quote/p.quote_purchase - 1) < 0