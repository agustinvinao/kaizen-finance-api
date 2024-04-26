
# Advanced Data Analysis in PostgreSQL: Statistical Properties Explored

url: https://airbyte.com/blog/advanced-data-analysis-in-postgresql

```sql
SELECT symbol,
	var_samp(close), -- sample variance
	stddev_samp(close), -- standard deviation is computed using stddev_samp
	stddev_samp(close) / avg(close) AS coefficient_of_variation -- The coefficient of variation (CV) is a commonly used metric - it denotes the dispersion of the data relative to the mean

FROM ticks_1h t
WHERE
     t.symbol = 'AAVEUSD' AND t.dt > date_trunc('hour', now() - INTERVAL '2 days') 
GROUP BY symbol
```

# A Stock Price Correlation Matrix

url: https://www.sqlservercentral.com/articles/a-stock-price-correlation-matrix

# several calculations
https://towardsdatascience.com/how-to-derive-summary-statistics-using-postgresql-742f3cdc0f44