WHEN pivot = 1 AND
			high > LEAD(high) OVER (PARTITION BY symbol, pivot ORDER BY dt) AND
			high > LAG(high) OVER (PARTITION BY symbol, pivot ORDER BY dt)
		THEN 'HH'
		WHEN pivot = -1 AND
			low < LEAD(low) OVER (PARTITION BY symbol, pivot ORDER BY dt) AND
			low < LAG(low) OVER (PARTITION BY symbol, pivot ORDER BY dt)
		THEN 'LL'