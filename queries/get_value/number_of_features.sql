SELECT COUNT(DISTINCT column_name) - 2
FROM information_schema.columns
WHERE table_name = 'model_development_train'
