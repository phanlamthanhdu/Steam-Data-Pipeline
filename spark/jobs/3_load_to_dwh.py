from pyspark.sql import SparkSession

GOLDER_DIR = "s3a://steam-datalake-duplt/golden/"

# Get Spark instance
spark = (
    SparkSession
        .builder
        .appName("ContentProcess")
        .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .getOrCreate()
)
# Load golden tables as DataFrames
df_dim_game = spark.read.parquet(GOLDER_DIR + "dim_game")
df_dim_review_text = spark.read.parquet(GOLDER_DIR + "dim_review_text")
df_dim_date = spark.read.parquet(GOLDER_DIR + "dim_date")
df_bridge_game_dlc = spark.read.parquet(GOLDER_DIR + "bridge_game_dlc")
df_bridge_game_production = spark.read.parquet(GOLDER_DIR + "bridge_game_production")
df_bridge_game_genre = spark.read.parquet(GOLDER_DIR + "bridge_game_genre")
df_fact_review = spark.read.parquet(GOLDER_DIR + "fact_review")

# Url to dwh
jdbc_url = "jdbc:postgresql://postgres:5432/steam_dwh"

properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

tables = [
    ("dim_game", df_dim_game),
    ("dim_review_text", df_dim_review_text),
    ("dim_date", df_dim_date),
    ("bridge_game_dlc", df_bridge_game_dlc),
    ("bridge_game_production", df_bridge_game_production),
    ("bridge_game_genre", df_bridge_game_genre),
    ("fact_review", df_fact_review)
]

for table_name, df in tables:
    print(f"Writing {table_name} . . .", end=" ")
    df.write \
        .jdbc(
            url=jdbc_url,
            table=table_name,
            mode="append",
            properties=properties
        )
    print("COMPELETED")