DROP TABLE longterm_boundaries;

CREATE TABLE longterm_boundaries (
	symbol TEXT PRIMARY KEY,
	start_at DATE NOT NULL,
	end_at DATE NOT NULL
);
CREATE INDEX ON longterm_boundaries (symbol);