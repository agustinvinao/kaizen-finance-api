DROP TYPE IF EXISTS asset_category;
CREATE TYPE asset_category AS ENUM ('crypto','debt','hybrid','derivative','equity','stock','mutual_found','bonds','index','etf','fx');

DROP TABLE IF NOT EXISTS assets;
CREATE TABLE IF NOT EXISTS assets (
  symbol TEXT NOT NULL,
  name TEXT NOT NULL,
  exchange TEXT NOT NULL,
  category asset_category NOT NULL DEFAULT 'stock',
	PRIMARY KEY (symbol)
);