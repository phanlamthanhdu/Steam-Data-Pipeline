CREATE TABLE bridge_game_dlc (
    dlc_id   BIGINT PRIMARY KEY,
    game_id  BIGINT NOT NULL,
    dlc      TEXT,
    FOREIGN KEY (game_id) REFERENCES dim_game(game_id)
);