DROP TYPE IF EXISTS transaction_category;
CREATE TYPE transaction_category AS ENUM ('buy', 'deposit', 'dividend', 'fee', 'interest', 'profit', 'sell', 'taxes', 'withdraw');

DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
	id SERIAL PRIMARY KEY,
	symbol TEXT,
  created_at TIMESTAMPTZ NOT NULL,
	comment TEXT,
	amount DOUBLE PRECISION NOT NULL,
	quote DOUBLE PRECISION NOT NULL,
	shares DOUBLE PRECISION NOT NULL,
	shares_left DOUBLE PRECISION NOT NULL,
	fee DOUBLE PRECISION NOT NULL,
	taxes DOUBLE PRECISION NOT NULL,
	currency TEXT NOT NULL,
	category transaction_category DEFAULT 'buy',
	broker TEXT NOT NULL,
	broker_id TEXT NOT NULL
);
	-- PRIMARY KEY (dt)
  -- CONSTRAINT fk_assets FOREIGN KEY (symbol) REFERENCES assets (symbol)
CREATE INDEX ON transactions (created_at DESC);
SELECT create_hypertable('transactions', 'dt'); -- by_time('dt'), if_not_exists => TRUE