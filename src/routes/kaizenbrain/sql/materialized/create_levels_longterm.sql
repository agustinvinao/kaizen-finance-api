CREATE MATERIALIZED VIEW levels_longterm AS
	WITH beginning AS (SELECT  t.* FROM longterm_boundaries ll INNER JOIN ticks_1w t ON ll.symbol = t.symbol AND date_trunc('week', t.dt) = date_trunc('week', ll.start_at)),
		 ending AS (SELECT  t.* FROM longterm_boundaries ll INNER JOIN ticks_1w t ON ll.symbol = t.symbol AND date_trunc('week', t.dt) = date_trunc('week', ll.end_at)),
		 boundaries AS (
			SELECT ll.symbol,
				CASE WHEN b.high > e.high THEN b.high WHEN b.high < e.high THEN e.high END AS top,
				CASE WHEN b.low < e.low THEN b.low WHEN b.low > e.low THEN e.low END AS bottom
			FROM longterm_boundaries ll JOIN beginning b ON ll.symbol = b.symbol JOIN ending e ON ll.symbol = e.symbol

		 )
	SELECT 
			b.*,
			unnest(array[0, 0.236, 0.382, 0.5, 0.618, 0.812, 1, 1.236, 1.618, 2.618, 4.236, 8.486, 11.09]) as level,
			unnest(array[
				b.top - b.bottom,
				b.bottom + (b.top - b.bottom) * 0.236,
				b.bottom + (b.top - b.bottom) * 0.382,
				b.bottom + (b.top - b.bottom) * 0.5,
				b.bottom + (b.top - b.bottom) * 0.618,
				b.bottom + (b.top - b.bottom) * 0.812,
				b.bottom + (b.top - b.bottom),
				b.bottom + (b.top - b.bottom) * 1.236,
				b.bottom + (b.top - b.bottom) * 1.618,
				b.bottom + (b.top - b.bottom) * 2.618,
				b.bottom + (b.top - b.bottom) * 4.236,
				b.bottom + (b.top - b.bottom) * 8.486,
				b.bottom + (b.top - b.bottom) * 11.09
			]) as price
		FROM longterm_boundaries ll JOIN boundaries b ON ll.symbol = b.symbol
		
		
-- DROP VIEW levels_longterm
REFRESH MATERIALIZED VIEW levels_longterm;
DROP MATERIALIZED VIEW levels_longterm;