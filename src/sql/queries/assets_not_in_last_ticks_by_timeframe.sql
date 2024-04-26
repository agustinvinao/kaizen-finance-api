SELECT
  DISTINCT symbol
FROM
  assets
WHERE
  category = 'stock'
  AND symbol NOT IN (
    SELECT
      DISTINCT symbol
    FROM
      last_ticks_1d
  )