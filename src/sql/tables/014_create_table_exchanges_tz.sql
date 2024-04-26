DROP TABLE IF EXISTS exchanges_tz;
CREATE TABLE IF NOT EXISTS exchanges_tz (
  exchange TEXT NOT NULL,
  tz TEXT NOT NULL,
	PRIMARY KEY (exchange)
);

INSERT INTO exchanges_tz VALUES
('BITSTAMP','UTC'),
('EURONEXT','Europe/Berlin'),
('KRAKEN','UTC'),
('LSE','Europe/Londong'),
('NASDAQ','America/New_York'),
('NYSE','America/New_York'),
('XETR','Europe/Berlin');