-- https://maxhalford.github.io/blog/sql-cross-correlations/
WITH data AS (
	SELECT symbol, dt as date, close as y, open as x FROM ticks_1h t WHERE symbol = 'AAVEUSD' AND dt::date > (now() - '3 day'::interval)
)
, delta AS (
    SELECT * FROM generate_series(1, 3) delta
)
, dates AS (
    SELECT date AS present, delta, date + delta * '1 hour'::interval AS future
    FROM data CROSS JOIN delta
)
, pairwise AS (
    SELECT
        dates.present, dates.future, dates.delta, present.x AS present_x, future.y AS future_y
    FROM
        dates,
        data AS present,
        data AS future
    WHERE dates.present = present.date AND dates.future = future.date
)
, cross_corrs AS (
    SELECT
        delta,
        CORR(present_x, future_y) AS pearson
    FROM pairwise
    GROUP BY delta
    ORDER BY delta
)

, pairwise_ranks AS (
    SELECT
    	delta,
        RANK () OVER (
            PARTITION BY delta
            ORDER BY present_x
        ) AS present_x_rank,
        RANK () OVER (
            PARTITION BY delta
            ORDER BY future_y
        ) AS future_y_rank
    FROM pairwise
)

, cross_rank_corrs AS (
    SELECT
        delta,
        CORR(present_x_rank, future_y_rank) AS spearman
    FROM pairwise_ranks
    GROUP BY delta
    ORDER BY delta
)

, corrs AS (
    SELECT
        cc.delta,
        cc.pearson,
        crc.spearman
    FROM cross_corrs cc
    LEFT JOIN cross_rank_corrs crc
        ON cc.delta = crc.delta
)

SELECT * FROM corrs;