CREATE SCHEMA itmes;

CREATE TABLE items.symbol (
    symbol_id SERIAL,
    symbol varchar(10) PRIMARY KEY,
    name varchar(255),
    ihub_code varchar(255),
      -- REGEXP_REPLACE(ihub_code,'.*-','')::INTEGER
    ihub_id int,
    status varchar(10) DEFAULT 'active',
    created_date timestamp DEFAULT current_timestamp,
    modified_date timestamp
);

CREATE TABLE items.changed_symbol (
    symbol_id int,
    changed_from varchar(10),
    changed_to varchar(10),
    date_modified timestamp
);

CREATE SCHEMA ihub;

CREATE TABLE ihub.message_board (
    symbol_id int,
    post_number int,
    subject varchar(255),
    username varchar(100),
    post_time timestamp,
    sentiment_polarity float8,
    sentiment_subjectivity float8,
    PRIMARY KEY (symbol_id, post_number)
);

CREATE TABLE ihub.message_sentiment (
    status varchar(20),
    message_id int PRIMARY KEY,
    ihub_code varchar(255),
    post_number int,
    sentiment_polarity float8,
    sentiment_subjectivity float8,
    message_date timestamp,
    created_date timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_date timestamp
);

CREATE TABLE market.price_history (
    symbol_id int,
    date date,
    open float8,
    high float8,
    low float8,
    close float8,
    volume int,
    PRIMARY KEY (symbol_id, date)
);

CREATE SCHEMA model;

CREATE TABLE model.combined_data (
    idx SERIAL,
    symbol_id int,
    date date,
    ohlc_average float8,
    dollar_volume float8,
    num_posts int,
    defined_target float4,
    pred_eligible bool DEFAULT TRUE,
    working_train bool DEFAULT FALSE,
    working_validation bool DEFAULT FALSE,
    model_development_train bool DEFAULT FALSE,
    model_development_test bool DEFAULT FALSE,
    model_development_prediction float4,
    final_train bool DEFAULT FALSE,
    final_validation bool DEFAULT FALSE,
    final_prediction float4,
    created_date timestamp DEFAULT current_timestamp,
    modified_date timestamp,
    PRIMARY KEY (symbol_id, date)
);

-- ALTER TABLE model.combined_data
    -- DROP COLUMN working_model,
    -- DROP COLUMN final_model;
    -- ADD COLUMN working_model int,
    -- ADD COLUMN final_model int;
    -- ADD COLUMN working_validation int,
    -- ADD COLUMN final_validation int
    ;
