DROP TABLE IF EXISTS indicators_4h;
CREATE TABLE IF NOT EXISTS indicators_4h (
  symbol TEXT NOT NULL,
  dt TIMESTAMP WITH TIME ZONE NOT NULL,
  sma_fast DOUBLE PRECISION NOT NULL, 
  sma_slow DOUBLE PRECISION NOT NULL,
  atr DOUBLE PRECISION NOT NULL,
  atr_window DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (symbol, dt)
);
CREATE INDEX ON indicators_4h (symbol, dt DESC);
SELECT create_hypertable('indicators_4h', 'dt');

-- enable compression
ALTER TABLE indicators_4h SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'dt',
    timescaledb.compress_chunk_time_interval = '1 week');
