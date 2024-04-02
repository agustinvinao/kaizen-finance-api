DROP TABLE IF NOT EXISTS portfolios;
CREATE TABLE IF NOT EXISTS portfolios (
  code TEXT NOT NULL,
  name TEXT NOT NULL,
	PRIMARY KEY (code)
);


insert into portfolios (code, name) values ('short', ''), ('long', '2 years vista');