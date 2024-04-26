CREATE MATERIALIZED VIEW crypto_ticks_1h AS
	SELECT t.*
  FROM assets a
    JOIN ticks_1h t ON a.symbol = t.symbol
  WHERE category = 'crypto'::asset_category

-- update view
CALL refresh_continuous_aggregate('crypto_ticks_1h', '2020-01-01', '2021-01-01');

-- drop materialized view
-- DROP MATERIALIZED VIEW IF EXISTS ticks_4h;

