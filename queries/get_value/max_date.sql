SELECT MAX(date)
FROM market.price_history
WHERE symbol_id = {symbol_id}
