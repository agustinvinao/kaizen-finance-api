WITH demark_pivot_points AS (
	SELECT
	dpp_t1d.symbol,
	dpp_t1d.dt::date as dt,
	CASE 
		WHEN dpp_t1d.close < dpp_t1d.open THEN (dpp_t1d.high + (2 * dpp_t1d.low) + dpp_t1d.close)
		WHEN dpp_t1d.close > dpp_t1d.open then ((2 * dpp_t1d.high) + dpp_t1d.low + dpp_t1d.close)
		WHEN dpp_t1d.close = dpp_t1d.open then (dpp_t1d.high + dpp_t1d.low + (2 * dpp_t1d.close))
	END as x
	FROM ticks_1d dpp_t1d )

SELECT
	t1d.symbol,
	t1d.dt::DATE,
	t1d.open,
	t1d.high,
	t1d.low,
	t1d.close,
	dpp.x  / 4 AS pivot_point,
	dpp.x / 2 - t1d.high AS support,
	dpp.x / 2 - t1d.low AS resistance
FROM ticks_1d t1d JOIN demark_pivot_points dpp ON t1d.symbol = dpp.symbol AND t1d.dt::date = dpp.dt
WHERE t1d.symbol = 'PAYC'
ORDER BY t1d.dt DESC
LIMIT 10