INSERT INTO model.combined_data
SET defined_target = {defined_target}
WHERE {symbol_id} = symbol_id
    AND {date} = date;
