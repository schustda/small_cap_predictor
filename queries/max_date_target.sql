SELECT MAX(date) FROM (
    SELECT date
    FROM model.combined_data
    WHERE symbol_id = {symbol_id}
    ORDER BY date
    LIMIT {num}
) t1;
