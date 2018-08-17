SELECT
    symbol_id,
    count(*)
FROM
    model.combined_data
GROUP BY
    1
    ;
