SELECT s.symbol, s.exchange, DATE_TRUNC('hour', now()) - quote_last_quote_at AS datediff
FROM securities s INNER JOIN positions p ON s.symbol = p.symbol
WHERE quote_last_quote_at < DATE_TRUNC('hour', now())
