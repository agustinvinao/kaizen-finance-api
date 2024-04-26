DROP TABLE IF NOT EXISTS portfolio_assets;
CREATE TABLE IF NOT EXISTS portfolio_assets (
  code TEXT NOT NULL,
  symbol TEXT NOT NULL,
	PRIMARY KEY (code, symbol)
);

insert into portfolio_assets (symbol, code) values
-- short
('AAOI', 'short'),
('ALB', 'short'),
('CRDF', 'short'),
('ESEA', 'short'),
('GCT', 'short'),
('HEI', 'short'),
('INMD', 'short'),
('MSTR', 'short'),
('NKTX', 'short'),
('PAYC', 'short'),
('SEDG', 'short'),
('SURG', 'short'),
('WDAY', 'short'),
-- long
('AAPL','long'),
('AMZN','long'),
('BABA','long'),
('CI','long'),
('CLS','long'),
('CSCO','long'),
('DBK','long'),
('GOOG','long'),
('JPM','long'),
('KO','long'),
('LQQ','long'),
('META','long'),
('MSFT','long'),
('NVDA','long'),
('ON','long'),
('PYPL','long'),
('SBUX','long'),
('T','long');

