SELECT
	p.symbol,
	p.amount AS amount,
	p.quote_purchase AS quote_purchase,
	p.quote_last_quote AS quote_last_quote, ticks_1d_ago.close AS ticks_yesterday,
	((p.quote_last_quote * p.shares) - p.amount) AS profit_historical,
	p.quote_last_quote/p.quote_purchase - 1 AS profit_historical_perc,
	p.shares * (ticks_1d_ago.close - p.quote_last_quote) AS profits_last_day,
	p.quote_last_quote/ticks_1d_ago.close - 1 AS profits_last_day_perc,
	p.amount/p_tot.total AS participation
FROM securities s
	INNER JOIN positions p ON s.symbol = p.symbol
	INNER JOIN LATERAL (
			SELECT close, bucket FROM ticks_1d t1d
				WHERE p.symbol = t1d.symbol  AND t1d.bucket < CURRENT_DATE
				ORDER BY bucket DESC LIMIT 1
		) AS ticks_1d_ago ON true
	JOIN LATERAL (SELECT SUM(amount) AS total FROM positions) AS p_tot ON true
WHERE s.category = 'stock' AND p.shares > 0
ORDER BY (p.quote_last_quote/ticks_1d_ago.close - 1)