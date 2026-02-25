-- Drop fact first
DROP TABLE IF EXISTS fact_review;

-- Drop bridge tables
DROP TABLE IF EXISTS bridge_game_genre;
DROP TABLE IF EXISTS bridge_game_production;
DROP TABLE IF EXISTS bridge_game_dlc;

-- Drop dimension tables
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_review_text;
DROP TABLE IF EXISTS dim_game;