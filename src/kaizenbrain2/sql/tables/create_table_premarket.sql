CREATE TABLE IF NOT EXISTS premarket (
	symbol TEXT NOT NULL,
	tf timeframe NOT NULL DEFAULT '1w',
	note TEXT NOT NULL,
	occurs_on TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (symbol, tf, occurs_on)
);
CREATE INDEX ON premarket (symbol, tf, occurs_on DESC);

CREATE TABLE IF NOT EXISTS premarket_actions (
	symbol TEXT NOT NULL,
	comment TEXT NOT NULL,
	occurs_on TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (symbol, created_at)
);
CREATE INDEX ON premarket_actions (symbol, created_at DESC);