CREATE TABLE bridge_game_production (
    prod_id   BIGINT PRIMARY KEY,
    game_id   BIGINT NOT NULL,
    publisher TEXT,
    developer TEXT,
    FOREIGN KEY (game_id) REFERENCES dim_game(game_id)
);