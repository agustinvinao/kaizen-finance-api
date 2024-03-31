CREATE TABLE IF NOT EXISTS ticks_1w (
  symbol TEXT NOT NULL,
  dt TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION NOT NULL, 
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL, 
  volume DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (symbol, dt)
);
-- CONSTRAINT fk_assets FOREIGN KEY (symbol) REFERENCES assets (symbol)
CREATE INDEX ON ticks_1w (symbol, dt DESC);
-- SELECT create_hypertable('ticks', by_range('dt'), if_not_exists => TRUE);
SELECT create_hypertable('ticks_1w', 'dt');

-- enable compression
ALTER TABLE ticks_1w SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'dt',
    timescaledb.compress_chunk_time_interval = '1 week');

-- add compression
-- SELECT add_compression_policy('ticks ', INTERVAL '1 week');