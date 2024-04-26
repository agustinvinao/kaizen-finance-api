SELECT id, html_url
FROM latest_filings lf
WHERE lf.id NOT IN (SELECT DISTINCT latest_filing_id FROM insider_trading)