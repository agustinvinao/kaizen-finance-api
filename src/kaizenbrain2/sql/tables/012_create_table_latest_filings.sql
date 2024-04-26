DROP TABLE IF EXISTS latest_filings;
CREATE TABLE IF NOT EXISTS latest_filings (
  id SERIAL PRIMARY KEY,
  html_url TEXT  NOT NULL,
  insider_name TEXT NOT NULL,
  accepted_datetime TIMESTAMP NOT NULL,
  fill_date DATE NOT NULL,
  file_code TEXT,
  film_no INTEGER,
  comment TEXT NOT NULL
);

DROP TABLE IF EXISTS insider_trading;
CREATE TABLE IF NOT EXISTS insider_trading (
  id SERIAL PRIMARY KEY,
  fill_date DATE NOT NULL,
  accepted_datetime TIMESTAMP NOT NULL,
  ticker TEXT NOT NULL,
  company_name TEXT NOT NULL,
  insider_name TEXT NOT NULL,
  title TEXT,
  trade_type TEXT NOT NULL,
  exercise_price FLOAT NOT NULL,
  date_exercisable TEXT,
  price FLOAT NOT NULL,
  qty FLOAT NOT NULL,
  qty_reported FLOAT NOT NULL,
  values FLOAT NOT NULL,
  form4_url TEXT NOT NULL,
  explanation TEXT,
  latest_filing_id INTEGER NOT NULL,
  CONSTRAINT fk_latest_filings FOREIGN KEY(latest_filing_id) REFERENCES latest_filings (id)
);


-- truncate table latest_filings RESTART IDENTITY;
-- truncate table insider_trading RESTART IDENTITY;