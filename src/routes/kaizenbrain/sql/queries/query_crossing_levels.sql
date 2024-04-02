-- DAILY
SELECT 	to_char(now() - p.updated_at, 'dd "days" ago, hh24 "hours" ago') AS ago,
		p.symbol,
		t1d.dt::date AS dt,
		la.level / 100 as level,
		CASE
        		WHEN t1d.close > t1d.open THEN 'bullish'
        		WHEN t1d.open > t1d.close THEN 'bearish'
        END AS direction,
        CASE
        		WHEN la.price BETWEEN t1d.close AND t1d.open THEN 'annual'
        		WHEN la.price BETWEEN t1d.open AND t1d.close THEN 'annual'
        END AS crossing,
        la.dist,
        p.quote_last_quote / la.price - 1 AS movement
FROM positions p
	CROSS JOIN LATERAL (SELECT *, CASE WHEN t1d.open > t1d.close THEN 'desc' WHEN t1d.open < t1d.close THEN 'asc' END AS direction
						FROM ticks_1d t1d WHERE p.symbol = t1d.symbol ORDER BY dt DESC LIMIT 1) t1d
	CROSS JOIN LATERAL (SELECT la.open_price, la.price, la.level, abs(t1d.close - la.price) / t1d.close AS dist
    						FROM levels_annual la WHERE p.symbol = la.symbol ORDER BY dist LIMIT 1) la
WHERE (la.price BETWEEN t1d.close AND t1d.open) OR (la.price BETWEEN t1d.open AND t1d.close)
UNION
SELECT 	to_char(now() - p.updated_at, 'dd "days" ago, hh24 "hours" ago') AS ago,
		p.symbol,
		t1d.dt::date AS dt,
		ll.level,
		CASE
        		WHEN t1d.close > t1d.open THEN 'bullish'
        		WHEN t1d.open > t1d.close THEN 'bearish'
        END AS direction,
        CASE
        		WHEN ll.price BETWEEN t1d.close AND t1d.open THEN 'longterm'
        		WHEN ll.price BETWEEN t1d.open AND t1d.close THEN 'longterm'
        END AS crossing,
        ll.dist,
        p.quote_last_quote / ll.price - 1 AS movement
FROM positions p
	CROSS JOIN LATERAL (SELECT *, CASE WHEN t1d.open > t1d.close THEN 'desc' WHEN t1d.open < t1d.close THEN 'asc' END AS direction
						FROM ticks_1d t1d WHERE p.symbol = t1d.symbol ORDER BY dt DESC LIMIT 1) t1d
	CROSS JOIN LATERAL (SELECT ll.price, ll.level, abs(t1d.close - ll.price) / t1d.close AS dist
    						FROM levels_longterm ll WHERE p.symbol = ll.symbol ORDER BY dist LIMIT 1) ll
WHERE (ll.price BETWEEN t1d.close AND t1d.open) OR (ll.price BETWEEN t1d.open AND t1d.close)
ORDER BY direction DESC, symbol

-- WEEKLY
SELECT 	to_char(now() - p.updated_at, 'dd "days" ago') AS ago,
		p.symbol,
		t.dt::date AS dt,
		la.level / 100 as level,
		CASE
        		WHEN t.close > t.open THEN 'bullish'
        		WHEN t.open > t.close THEN 'bearish'
        END AS direction,
        CASE
        		WHEN la.price BETWEEN t.close AND t.open THEN 'annual'
        		WHEN la.price BETWEEN t.open AND t.close THEN 'annual'
        END AS crossing,
        la.dist,
        p.quote_last_quote / la.price - 1 AS movement
FROM positions p
	CROSS JOIN LATERAL (SELECT *, CASE WHEN t.open > t.close THEN 'desc' WHEN t.open < t.close THEN 'asc' END AS direction
						FROM ticks_1w t WHERE p.symbol = t.symbol ORDER BY dt DESC LIMIT 1) t
	CROSS JOIN LATERAL (SELECT la.open_price, la.price, la.level, abs(t.close - la.price) / t.close AS dist
    						FROM levels_annual la WHERE p.symbol = la.symbol ORDER BY dist LIMIT 1) la
WHERE (la.price BETWEEN t.close AND t.open) OR (la.price BETWEEN t.open AND t.close)
UNION
SELECT 	to_char(now() - p.updated_at, 'dd "days" ago') AS ago,
		p.symbol,
		t.dt::date AS dt,
		ll.level,
		CASE
        	WHEN t.close > t.open THEN 'bullish'
        	WHEN t.open > t.close THEN 'bearish'
        END AS direction,
        CASE
        	WHEN ll.price BETWEEN t.close AND t.open THEN 'longterm'
        	WHEN ll.price BETWEEN t.open AND t.close THEN 'longterm'
        END AS crossing,
        ll.dist,
        p.quote_last_quote / ll.price - 1 AS movement
FROM positions p
	CROSS JOIN LATERAL (SELECT *, CASE WHEN t.open > t.close THEN 'desc' WHEN t.open < t.close THEN 'asc' END AS direction
						FROM ticks_1w t WHERE p.symbol = t.symbol ORDER BY dt DESC LIMIT 1) t
	CROSS JOIN LATERAL (SELECT ll.price, ll.level, abs(t.close - ll.price) / t.close AS dist
    					FROM levels_longterm ll WHERE p.symbol = ll.symbol ORDER BY dist LIMIT 1) ll
WHERE (ll.price BETWEEN t.close AND t.open) OR (ll.price BETWEEN t.open AND t.close)
ORDER BY direction DESC, symbol
