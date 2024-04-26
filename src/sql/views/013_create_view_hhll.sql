WITH add_row_number AS (
  SELECT dt, symbol, open, close, high, low,
  ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dt) AS pos
  FROM ticks_1w t WHERE symbol = 'PYPL'
)
, ticks_arrays AS (
  SELECT 
    dt, symbol, open, close, high, low, pos,
    CASE 
         WHEN pos < 24 THEN filter_hhll(ARRAY(SELECT high FROM add_row_number WHERE symbol = arn.symbol AND pos BETWEEN 0 AND arn.pos), TRUE)
       WHEN pos >= 23 THEN filter_hhll(ARRAY(SELECT high FROM add_row_number WHERE symbol = arn.symbol AND pos > (arn.pos - 22) LIMIT 22), TRUE)
    END AS array_highs,
    CASE 
       WHEN pos < 24 THEN filter_hhll(ARRAY(SELECT low FROM add_row_number WHERE symbol = arn.symbol AND pos between 0 and arn.pos), FALSE)
       WHEN pos >= 23 THEN filter_hhll(ARRAY(SELECT low FROM add_row_number WHERE symbol = arn.symbol AND pos > (arn.pos - 22) LIMIT 22), FALSE)
    END AS array_lows
  FROM add_row_number arn
)
, add_deques AS (
  SELECT dt, symbol, open, close, high, low, pos,
         (array_highs)[1] AS deque_high, (array_lows)[1] AS deque_low
  FROM ticks_arrays
)
, pivot_types AS (
  SELECT dt, symbol, open, close, high, low, pos, deque_high, deque_low,
    CASE 
      WHEN pos > 22 AND LEAD(deque_low, 11) OVER (PARTITION BY symbol ORDER BY dt) = low THEN 'PL'
      WHEN pos > 23 AND LEAD(deque_high, 11) OVER (PARTITION BY symbol ORDER BY dt) = high THEN 'PH'
    END AS pivot_type,
    CASE 
      WHEN pos > 22 AND LEAD(deque_low, 11) OVER (PARTITION BY symbol ORDER BY dt) = low THEN low
      WHEN pos > 23 AND LEAD(deque_high, 11) OVER (PARTITION BY symbol ORDER BY dt) = high THEN high
    END AS pivot_type_value
  FROM add_deques
)
, pivot_type_rows AS (
  SELECT dt, symbol,
    pivot_type, pivot_type_value,
    LAG(pivot_type_value) OVER (PARTITION BY symbol, pivot_type ORDER BY dt) as pivot_type_value_prev,
    LEAD(pivot_type_value) OVER (PARTITION BY symbol, pivot_type ORDER BY dt) as pivot_type_value_post,
    CASE
      WHEN pivot_type = 'PH' AND pivot_type_value > LAG(pivot_type_value)         OVER (PARTITION BY symbol, pivot_type ORDER BY dt) THEN 'HH'
      WHEN pivot_type = 'PH' AND pivot_type_value < LAG(pivot_type_value)         OVER (PARTITION BY symbol, pivot_type ORDER BY dt) THEN 'LH'
      WHEN pivot_type = 'PH' AND pivot_type_value = FIRST_VALUE(pivot_type_value) OVER (PARTITION BY symbol, pivot_type ORDER BY dt) THEN 'H'
      WHEN pivot_type = 'PL' AND pivot_type_value < LAG(pivot_type_value)         OVER (PARTITION BY symbol, pivot_type ORDER BY dt) THEN 'LL'
      WHEN pivot_type = 'PL' AND pivot_type_value > LAG(pivot_type_value)         OVER (PARTITION BY symbol, pivot_type ORDER BY dt) THEN 'HL'
      WHEN pivot_type = 'PL' AND pivot_type_value = FIRST_VALUE(pivot_type_value) OVER (PARTITION BY symbol, pivot_type ORDER BY dt) THEN 'L'
    END pivot_type_detail
  FROM pivot_types WHERE pivot_type IS NOT NULL
)
SELECT pt.dt, pt.symbol, pt.open, pt.close, pt.high, pt.low, pt.pos,
       deque_high, deque_low, 
       ptr.pivot_type, ptr.pivot_type_value, ptr.pivot_type_detail
FROM pivot_types pt
  LEFT JOIN pivot_type_rows ptr ON pt.dt=ptr.dt AND pt.symbol=ptr.symbol
ORDER BY pt.dt
