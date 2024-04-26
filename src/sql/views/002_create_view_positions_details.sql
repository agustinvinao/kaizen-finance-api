CREATE VIEW positions_details AS
SELECT
	p.symbol,
	p.totalstock,
	p.costbasis,
	p.totalstock * t.close AS marketvalue,
	p.totalstock * t.close - p.costbasis AS diff,
	p.avgprice,
	t.close AS last,
	t.close/p.avgprice - 1 as profit_perc,
	justify_interval(now() - p.created_at) AS date_diff
FROM positions p LEFT JOIN last_ticks_1d t ON p.symbol = t.symbol
ORDER BY symbol