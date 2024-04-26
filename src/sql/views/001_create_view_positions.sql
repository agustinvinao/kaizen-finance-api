CREATE VIEW positions AS

/* Sum up the ins and outs to calculate the remaining stock level */

WITH cteTransactions
  AS (
  	SELECT 	t.*
  	FROM 	assets a INNER JOIN transactions t ON a.symbol = t.symbol
   	WHERE 	a.category = 'stock' AND t.category IN ('buy','sell')
  )
, cteStockSum
  AS ( SELECT   t.symbol ,
                SUM(shares) AS TotalStock,
                SUM(amount + fee) AS TotalAmount,
                SUM(fee) AS TotalFee
       FROM     cteTransactions t
       GROUP BY t.symbol
  )
, cteReverseInSum
AS ( 	SELECT	s.symbol ,
               	s.created_at,
               	sum(s.shares) over (PARTITION BY symbol ORDER BY s.created_at ROWS BETWEEN current ROW AND unbounded following) AS RollingStock,
               	sum(s.amount + s.fee) over (PARTITION BY symbol ORDER BY s.created_at ROWS BETWEEN current ROW AND unbounded following) AS RollingAmount,
               	sum(s.fee) over (PARTITION BY symbol ORDER BY s.created_at ROWS BETWEEN current ROW AND unbounded following) AS Rollingfee,
               	s.shares AS ThisStock,
               	s.amount as ThisAmount,
               	s.fee as ThisFee
       	FROM    cteTransactions AS s
       	WHERE   s.shares > 0
  )
/* Using the rolling balance above find the first stock movement in that meets (or exceeds) our required stock level */
/* and calculate how much stock is required from the earliest stock in */
, cteWithLastTranDate
AS (		SELECT DISTINCT 
        			w.symbol,
        			w.TotalStock,
        			w.TotalAmount,
        			w.TotalFee,
        			LAST_VALUE(z.RollingAmount) OVER (PARTITION BY w.symbol ORDER BY z.created_at ROWS BETWEEN current ROW AND unbounded following) AS rolling_amount,
        			LAST_VALUE(created_at) OVER (PARTITION BY w.symbol ORDER BY z.created_at ROWS BETWEEN current ROW AND unbounded following) AS created_at
        	FROM cteStockSum w JOIN cteReverseInSum z ON w.symbol = z.symbol AND z.RollingStock >= w.TotalStock
  )
, cteData
AS ( 	SELECT  
		    y.symbol,
		    e.created_at,
		    e.TotalStock,
		    SUM(CASE 
		        WHEN e.created_at = y.created_at THEN e.TotalStock - (y.RollingStock - y.ThisStock)
		    		ELSE y.ThisStock END * Price.quote) AS CurrentAmount
		FROM cteReverseInSum AS y
			JOIN cteWithLastTranDate e ON e.symbol = y.symbol
			CROSS JOIN LATERAL ( 
				/* Find the Price of the item in */
				SELECT p.quote
				FROM cteTransactions AS p 
				WHERE   p.symbol = e.symbol AND p.created_at <= e.created_at AND p.shares > 0
				ORDER BY p.created_at DESC LIMIT 1
		) AS Price
		WHERE  y.created_at >= e.created_at AND e.TotalStock > 0
		GROUP BY y.symbol, e.TotalStock, e.created_at
		ORDER BY y.symbol
)
SELECT 
	d.symbol, d.created_at, d.TotalStock,
	ltd.rolling_amount as CostBasis,
	ltd.rolling_amount / d.TotalStock AS AvgPrice
FROM cteData d INNER JOIN cteWithLastTranDate ltd ON d.symbol = ltd.symbol