with temp as (SELECT *
    FROM model.combined_data
    WHERE idx = {idx})

SELECT *
FROM model.combined_data
WHERE symbol_id = (SELECT symbol_id FROM temp)
AND date <= (SELECT date from temp)
ORDER BY date DESC
LIMIT {num_days}
