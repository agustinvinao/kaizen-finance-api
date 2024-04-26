CREATE MATERIALIZED VIEW last_ticks_4h AS
SELECT t.*
FROM ticks_4h t
	INNER JOIN (SELECT symbol, max(dt) AS dt FROM ticks_4h GROUP BY symbol) AS last_items
	ON t.symbol = last_items.symbol AND t.dt = last_items.dt



REFRESH MATERIALIZED VIEW last_ticks_4h;
DROP MATERIALIZED VIEW last_ticks_4h;