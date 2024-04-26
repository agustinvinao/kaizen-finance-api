SELECT DISTINCT ao.symbol
FROM assets_observability ao JOIN insider_trading it ON ao.symbol = it.ticker
ORDER BY ao.symbol

SELECT * FROM insider_trading WHERE id IN (
  SELECT DISTINCT ao.symbol
  FROM assets_observability ao JOIN insider_trading it ON ao.symbol = it.ticker
  ORDER BY ao.symbol)


SELECT DISTINCT ticker, accepted_datetime::DATE as date, trade_type, sum(values::numeric::money) --, insider_name
FROM insider_trading
WHERE accepted_datetime::DATE = now()::DATE
GROUP BY ticker, trade_type, date --, insider_name
--, insider_name, title
-- ORDER BY accepted_datetime desc
--, , sum(values::numeric::money), insider_name, title

-- select * from insider_trading where ticker = 'DXYZ'





-- NON PROCESSED filings
SELECT id, html_url
FROM latest_filings lf
WHERE lf.id NOT IN (SELECT DISTINCT latest_filing_id FROM insider_trading)



SELECT DISTINCT	
	max(accepted_datetime) as accepted_datetime,
	ticker,
	sum(values)::NUMERIC::MONEY as investment,
	trade_type,
	insider_name,
	title
FROM insider_trading
WHERE accepted_datetime::date = '2024-04-08'
GROUP BY trade_type, insider_name, ticker,title
ORDER BY ticker,
		 insider_name,
		 trade_type,
		 investment desc,
		 accepted_datetime desc



SELECT DISTINCT	 ticker, accepted_datetime::date
FROM insider_trading
WHERE accepted_datetime::date = now()::date
ORDER by accepted_datetime desc