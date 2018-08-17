UPDATE model.combined_data
SET num_posts = (SELECT COUNT(*) FROM ihub.message_board WHERE symbol_id = {symbol_id} AND date_trunc('day',post_time) = '{date}'),
modified_date = '{modified_date}'
WHERE symbol_id = {symbol_id} AND date = '{date}'
;
