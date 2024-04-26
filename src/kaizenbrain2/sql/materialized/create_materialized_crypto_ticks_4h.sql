CREATE MATERIALIZED VIEW crypto_ticks_4h AS
	SELECT t.*
  FROM assets a
    JOIN ticks_4h t ON a.symbol = t.symbol
  WHERE category = 'crypto'::asset_category

-- update view
CALL refresh_continuous_aggregate('cyrpto_ticks_4h', '2020-01-01', '2021-01-01');

-- drop materialized view
-- DROP MATERIALIZED VIEW IF EXISTS ticks_4h;

