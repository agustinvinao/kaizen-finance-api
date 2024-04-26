CREATE TABLE IF NOT EXISTS ticks_4h (
  symbol TEXT NOT NULL,
  dt TIMESTAMP WITH TIME ZONE NOT NULL,
  open DOUBLE PRECISION NOT NULL, 
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL, 
  volume DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (symbol, dt)
);
-- CONSTRAINT fk_assets FOREIGN KEY (symbol) REFERENCES assets (symbol)
CREATE INDEX ON ticks_4h (symbol, dt DESC);
-- SELECT create_hypertable('ticks', by_range('dt'), if_not_exists => TRUE);
SELECT create_hypertable('ticks_4h', 'dt');

-- enable compression
ALTER TABLE ticks_4h SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'dt',
    timescaledb.compress_chunk_time_interval = '1 week');

-- add compression
-- SELECT add_compression_policy('ticks ', INTERVAL '1 week');