DROP TABLE longterm_boundaries;

CREATE TABLE longterm_boundaries (
	symbol TEXT PRIMARY KEY,
	start_at DATE NOT NULL,
	end_at DATE NOT NULL
);
CREATE INDEX ON longterm_boundaries (symbol);


INSERT INTO longterm_boundaries ("symbol", "start_at", "end_at") VALUES
('AAOI', '2020-08-03', '2022-07-11'),
('AAPL', '2022-01-03', '2023-01-03'),
('ALB', '2020-03-23', '2022-11-07'),
('BABA', '2022-07-05', '2022-10-24'),
('CI', '2021-11-29', '2022-12-12'),
('CLS', '2020-03-09', '2022-02-07'),
('CRDF', '2023-07-03', '2023-08-07'),
('CSCO', '2020-03-16', '2021-12-27'),
('DBK', '2022-02-07', '2022-10-03'),
('ESEA', '2021-09-20', '2023-03-13'),
('GCT', '2022-08-22', '2022-10-31'),
('GOOG', '2022-01-31', '2022-10-31'),
('INMD', '2019-11-18', '2020-03-16'),
('JPM', '2021-10-25', '2022-10-10'),
('KO', '2022-04-25', '2022-10-10'),
('LQQ', '2021-11-22', '2022-12-27'),
('META', '2020-03-16', '2021-08-30'),
('MSFT', '2021-11-22', '2022-10-31'),
('NKTX', '2022-04-15', '2023-10-03'),
('NVDA', '2021-11-22', '2022-10-10'),
('ON', '2022-01-03', '2022-07-05'),
('PAYC', '2021-11-01', '2022-06-13'),
('PYPL', '2022-08-15', '2022-05-22'),
('SBUX', '2021-07-19', '2022-05-09'),
('SEDG', '2021-11-22', '2022-10-10'),
('T', '2019-11-18', '2020-03-20'),
('WDAY', '2021-11-15', '2022-10-31'),
('HEI', '2021-09-07', '2023-05-15'),
('SURG', '2022-01-10', '2022-12-05'),
('MSTR', '2024-01-02', '2024-01-22'),
('AMZN', '2021-03-01', '2021-07-12');