CREATE VIEW last_ticks_1d AS
SELECT t.*
FROM ticks_1d t
	INNER JOIN (SELECT symbol, max(dt) AS dt FROM ticks_1d GROUP BY symbol) AS last_items
	ON t.symbol = last_items.symbol AND t.dt = last_items.dt



DROP VIEW last_ticks_1d;