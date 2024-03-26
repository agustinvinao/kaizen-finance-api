SELECT
  bucket,
  avg(close) OVER (order by bucket ROWS BETWEEN 19 PRECEDING AND current ROW) AS sma_20
FROM daily_bars
WHERE symbol=''
ORDER BY bucket DESC;
