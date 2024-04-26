-- CREATE TYPE period_ AS ENUM ('from', 'to');

DROP TABLE longterm_levels;
CREATE TABLE longterm_levels (
	symbol TEXT PRIMARY KEY,
	occurs_on DATE NOT NULL,
	-- end_at DATE NOT NULL
);
CREATE INDEX ON longterm_levels (symbol);