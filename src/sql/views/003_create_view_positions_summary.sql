CREATE VIEW positions_summary AS
SELECT
	SUM(costbasis) AS costbasis,
	SUM(marketvalue) AS marketvalue,
	SUM(marketvalue) / SUM(costbasis) - 1 AS profit_perc,
	SUM(marketvalue) - SUM(costbasis) AS diff
FROM positions_details