CREATE TABLE bridge_game_genre (
    genre_id  BIGINT PRIMARY KEY,
    game_id   BIGINT NOT NULL,
    genre     TEXT,
    FOREIGN KEY (game_id) REFERENCES dim_game(game_id)
);