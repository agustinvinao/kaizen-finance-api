DROP TABLE IF NOT EXISTS assets_observability;
CREATE TABLE IF NOT EXISTS assets_observability (
  symbol TEXT NOT NULL,
  tf timeframe NOT NULL,
	PRIMARY KEY (symbol, tf)
);

