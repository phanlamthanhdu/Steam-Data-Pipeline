CREATE TABLE dim_review_text (
    review_text_id  BIGINT PRIMARY KEY,
    review_hash     VARCHAR(64) UNIQUE NOT NULL, -- SHA256 = 64 characters → VARCHAR(64)
    text            TEXT,
    language        VARCHAR(20)
);