CREATE TABLE dim_game (
    game_id        BIGINT PRIMARY KEY,
    name           TEXT,
    type           TEXT,
    required_age   VARCHAR(10),
    price          INT,
    currency       VARCHAR(10),
    rating_value   NUMERIC(5,2),
    review_count   INT,
    status         TEXT,
    release_date   DATE
);