CREATE VIEW phases_current AS
SELECT 	a.symbol,
		p1w.direction AS direction_w, 	p1w.starting_on AS starting_on_w, 	p1w.ending_on AS ending_on_w,
		p1d.direction AS direction_d, 	p1d.starting_on AS starting_on_d, 	p1d.ending_on AS ending_on_d,
		p4h.direction AS direction_4h, 	p4h.starting_on AS starting_on_4h, 	p4h.ending_on AS ending_on_4h,
		CASE WHEN p1w.direction = 1 THEN '↓ ' WHEN p1w.direction = -1 THEN '↑ ' END ||
		CASE WHEN p1d.direction = 1 THEN '↓ ' WHEN p1d.direction = -1 THEN '↑ ' END ||
		CASE WHEN p4h.direction = 1 THEN '↓ ' WHEN p4h.direction = -1 THEN '↑' END AS phases_cols
FROM assets a  
	LEFT JOIN (SELECT symbol, direction, starting_on, ending_on, ROW_NUMBER() over(PARTITION BY symbol) AS row_num FROM phases_ticks_4h WHERE direction <> 0) AS p4h ON a.symbol = p4h.symbol
	LEFT JOIN (SELECT symbol, direction, starting_on, ending_on, ROW_NUMBER() over(PARTITION BY symbol) AS row_num FROM phases_ticks_1d WHERE direction <> 0) AS p1d ON a.symbol = p1d.symbol
	LEFT JOIN (SELECT symbol, direction, starting_on, ending_on, ROW_NUMBER() over(PARTITION BY symbol) AS row_num FROM phases_ticks_1w WHERE direction <> 0) AS p1w ON a.symbol = p1w.symbol
	LEFT JOIN (SELECT symbol, MAX(dt) as dt FROM ticks_1w GROUP BY symbol) AS p1wMax ON a.symbol = p1wMax.symbol
WHERE p4h.row_num = 2 AND  p1d.row_num = 2 AND p1w.row_num = 2 
