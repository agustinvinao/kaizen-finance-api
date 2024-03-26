------------
-- premarket
------------
INSERT INTO premarket (symbol, tf, occurs_on, note)
VALUES ('WDAY', '1w'::timeframe, DATE_TRUNC('week', now()) + '1 week'::interval, 'retrocedió de 38.2, moviendose entre 2 resistencias, posible toma de ganancias? fase 5?');

INSERT INTO premarket (symbol, tf, occurs_on, note)
VALUES ('T', '1d'::timeframe, DATE_TRUNC('week', now()) + '1 week'::interval, 'acumulando');

SELECT * FROM premarket WHERE symbol = 'AMZN'