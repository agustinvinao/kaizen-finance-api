SELECT
	p.symbol,
	to_char(p.amount, '999990D00') AS amount,
	to_char(p.quote_purchase, '999990D0099') AS quote_quote,
	to_char(p.quote_last_quote, '999990D0099') AS quote_last_quote,
	to_char(ticks_1d_ago.close, '999990D0099') AS ticks_yesterday_close,
	to_char(((p.quote_last_quote * p.shares) - p.amount), '999990D0099') AS profit_today_value,
	to_char((p.quote_last_quote/p.quote_purchase - 1) * 100, '990D99%') AS profits_today_perc,
	to_char((p.quote_last_quote/ticks_1d_ago.close - 1) * 100, '990D99%') AS profits_1d_perc
FROM securities s
	INNER JOIN positions p ON s.symbol = p.symbol
	INNER JOIN LATERAL (
	    SELECT close, bucket FROM ticks_1d t1d
	      WHERE p.symbol = t1d.symbol  AND t1d.bucket < CURRENT_DATE
	      ORDER BY bucket DESC LIMIT 1
	  ) AS ticks_1d_ago ON true
WHERE s.category = 'stock' AND p.shares > 0
ORDER BY (p.quote_last_quote/ticks_1d_ago.close - 1)
(p.quote_last_quote/p.quote_purchase - 1)