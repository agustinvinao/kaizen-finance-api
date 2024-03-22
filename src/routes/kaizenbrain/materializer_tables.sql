-- one day

-- vista
CREATE MATERIALIZED VIEW ticks_1d
WITH (timescaledb.continuous) AS
	SELECT
		symbol,
		time_bucket('1 day'::interval, dt) as bucket,
		first(open, dt) as open,
		MAX(high) as high,
		min(low) as low,
		last(close, dt) as close,
		SUM(volume) as volume
	FROM ticks
	GROUP BY symbol, bucket;

-- update view
CALL refresh_continuous_aggregate('ticks_1d', '2024-03-01', '2024-03-21');

-- drop materialized view
DROP MATERIALIZED VIEW IF EXISTS ticks_1d;


-- four hours

-- vista
CREATE MATERIALIZED VIEW four_hours_bars
WITH (timescaledb.continuous) AS
	SELECT
		security_id,
		time_bucket('4 hour'::interval, dt) as bucket,
		first(open, dt) as open,
		MAX(high) as high,
		min(low) as low,
		last(close, dt) as close,
		SUM(volume) as volume
	FROM ticks
	GROUP BY security_id, bucket;

-- update view
CALL refresh_continuous_aggregate('four_hours_bars', '2020-01-01', '2021-01-01');

-- drop materialized view
DROP MATERIALIZED VIEW IF EXISTS four_hours_bars;

