SELECT
    ohlc_average,
    dollar_volume,
    num_posts
FROM model.combined_data
WHERE
    date > {date}
ORDER BY date DESC
LIMIT {num_days}
