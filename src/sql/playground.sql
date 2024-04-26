WITH min_ticks AS (SELECT symbol, min(dt) as dt, count(*) as candles_count from ticks_4h group by symbol)

SELECT lb.symbol, start_at, t.dt, start_at < t.dt, (now()::date - start_at) * 4, candles_count,  (now()::date - start_at) * 4 > candles_count
FROM longterm_boundaries lb JOIN min_ticks t ON lb.symbol = t.symbol
