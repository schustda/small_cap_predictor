SELECT
    t1.symbol_id,
    t1.date,
    (open+high+low+close)/4 AS ohlc_average,
    (open+high+low+close)*volume/4 AS dollar_volume,
    CASE WHEN num_posts IS NULL THEN 0
    ELSE num_posts END AS num_posts
FROM (
    SELECT *
    FROM market.price_history
    WHERE symbol_id = {symbol_id}
    AND date NOT IN (SELECT date
      FROM model.combined_data
      WHERE symbol_id = {symbol_id}
    )
) t1

LEFT JOIN (
    SELECT
        symbol_id,
        date_trunc('day',post_time) AS date,
        COUNT(*) num_posts
    FROM ihub.message_board
    WHERE symbol_id = {symbol_id}
    GROUP BY 1, 2
) t2
ON t1.date = t2.date
-- LIMIT 2
;
