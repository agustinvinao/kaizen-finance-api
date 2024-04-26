DROP TABLE IF EXISTS ticks_waves_1w;
CREATE TABLE IF NOT EXISTS ticks_waves_1w (
  symbol TEXT NOT NULL,
  dt TIMESTAMP WITH TIME ZONE NOT NULL,
  pivot_high BOOLEAN, 
  pivot_high_value DOUBLE PRECISION,
  pivot_low BOOLEAN, 
  pivot_low_value DOUBLE PRECISION,
  PRIMARY KEY (symbol, dt)
);
CREATE INDEX ON ticks_waves_1w (symbol, dt DESC);
SELECT create_hypertable('ticks_waves_1w', 'dt');

-- enable compression
ALTER TABLE ticks_waves_1w SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'dt',
    timescaledb.compress_chunk_time_interval = '1 week');
