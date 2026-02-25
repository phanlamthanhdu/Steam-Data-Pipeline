CREATE TABLE dim_date (
    date_id   BIGINT PRIMARY KEY,
    date      DATE NOT NULL,
    year      INT,
    month     INT,
    day       INT,
    quarter   INT
);