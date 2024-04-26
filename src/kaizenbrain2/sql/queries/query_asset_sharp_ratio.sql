SELECT 
	t.symbol, t.bucket,
	t.open, t.high, t.low,
	t.close, returns.close as t1d_close,
	((returns.close/t.close) - 1) * 100  as diff, 
	t.volume,
	returns.ret,
	risk_free.val as risk_free,
	returns.ret - risk_free.val as excess_returns,
	avg(returns.ret - risk_free.val) OVER (PARTITION BY symbol) as mean_returns,
	stddev(returns.ret - risk_free.val) OVER (PARTITION BY symbol) as sd_returns,
	avg(returns.ret - risk_free.val) OVER (PARTITION BY symbol) / stddev(returns.ret - risk_free.val) OVER (PARTITION BY symbol) as sharpe_ratio
FROM ticks_1d t
	JOIN (SELECT 0.05 AS val) as risk_free ON true
	INNER JOIN LATERAL (
  		SELECT
  			t1d.close,
  			(t.close - t1d.close) as close_diff,
  			(t.close - t1d.close) / t1d.close*100 as ret
  		FROM ticks_1d t1d
      	WHERE t.symbol = t1d.symbol AND t1d.bucket = (t.bucket - '1 DAY'::interval)
      	ORDER BY bucket DESC LIMIT 1
	) AS returns ON true
WHERE t.symbol = 'AAOI'