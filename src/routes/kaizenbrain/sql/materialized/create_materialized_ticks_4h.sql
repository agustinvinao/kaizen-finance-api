CREATE MATERIALIZED VIEW ticks_4h
WITH (timescaledb.continuous) AS
	SELECT
		symbol,
		time_bucket('4 hour'::interval, dt) as bucket,
		first(open, dt) as open,
		MAX(high) as high,
		min(low) as low,
		last(close, dt) as close,
		SUM(volume) as volume
	FROM ticks
	GROUP BY symbol, bucket;

-- update view
CALL refresh_continuous_aggregate('ticks_4h', '2020-01-01', '2021-01-01');

-- drop materialized view
-- DROP MATERIALIZED VIEW IF EXISTS ticks_4h;

