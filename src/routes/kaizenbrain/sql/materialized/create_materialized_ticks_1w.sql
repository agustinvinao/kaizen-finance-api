-- vista
CREATE MATERIALIZED VIEW ticks_1w
WITH (timescaledb.continuous) AS
	SELECT
		symbol,
		time_bucket('1 week'::interval, dt) as bucket,
		first(open, dt) as open,
		MAX(high) as high,
		min(low) as low,
		last(close, dt) as close,
		SUM(volume) as volume
	FROM ticks
	GROUP BY symbol, bucket;

-- update view
CALL refresh_continuous_aggregate('ticks_1w', '2023-01-01', '2024-03-21');

-- drop materialized view
-- DROP MATERIALIZED VIEW IF EXISTS ticks_1w;