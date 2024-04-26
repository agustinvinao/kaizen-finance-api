DROP TABLE positions;
CREATE TABLE positions (
	symbol TEXT PRIMARY KEY,
	updated_at TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	shares DOUBLE PRECISION NOT NULL,
	quote_purchase DOUBLE PRECISION NOT NULL,
	quote_last_quote DOUBLE PRECISION,
	quote_last_quote_at TIMESTAMPTZ,
	fee DOUBLE PRECISION NOT NULL,
	amount DOUBLE PRECISION NOT NULL,
	currency TEXT NOT NULL
);
CREATE INDEX ON positions (symbol);