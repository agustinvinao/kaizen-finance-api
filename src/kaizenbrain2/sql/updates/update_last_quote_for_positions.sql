UPDATE positions
SET quote_last_quote=subquery.close, quote_last_quote_at=last_date
FROM (
	SELECT
	  t.symbol, last_date, t.close
	FROM ticks_4h t
		JOIN (
			SELECT DISTINCT
				symbol,
				MAX(dt) OVER (PARTITION BY symbol) as last_date
			FROM ticks_4h
		) AS s ON t.dt = s.last_date AND t.symbol = s.symbol
) AS subquery
WHERE positions.symbol = subquery.symbol;


UPDATE positions
SET quote_last_quote=subquery.close
FROM (SELECT t.symbol, dt, t.close FROM last_ticks_4h t order by ) AS subquery
WHERE positions.symbol = subquery.symbol;