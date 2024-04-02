SELECT *
FROM (SELECT symbol, direction, starting_on, ending_on, ROW_NUMBER() over(PARTITION BY symbol) AS row_num
	FROM phases_ticks_1d WHERE direction <> 0) AS phases
WHERE row_num = 2