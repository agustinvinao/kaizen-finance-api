UPDATE positions
SET quote_last_quote=subquery.close
FROM (
	SELECT
	  t.symbol, last_date, t.close
	FROM ticks t
		JOIN (
			SELECT DISTINCT
				symbol,
				MAX(dt) OVER (PARTITION BY symbol) as last_date
			FROM ticks
		) AS s ON t.dt = s.last_date AND t.symbol = s.symbol
) AS subquery
WHERE positions.symbol = subquery.symbol;