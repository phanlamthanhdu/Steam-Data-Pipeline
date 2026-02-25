CREATE TABLE fact_review (
    review_id       BIGINT PRIMARY KEY,
    game_id         BIGINT NOT NULL,
    review_text_id  BIGINT NOT NULL,
    date_id         BIGINT NOT NULL,
    recommended     BOOLEAN,
    score           DOUBLE,

    FOREIGN KEY (game_id) REFERENCES dim_game(game_id),
    FOREIGN KEY (review_text_id) REFERENCES dim_review_text(review_text_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);