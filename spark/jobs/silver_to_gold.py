from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_DIR = "s3a://steam-datalake-duplt/silver/"
GOLDER_DIR = "s3a://steam-datalake-duplt/golder/"
SILVER_CONTENT_EP = "stg_content"
SILVER_REVIEW_EP = "stg_review"

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

content_df = spark.read.parquet(SILVER_DIR + SILVER_CONTENT_EP)
review_df = spark.read.parquet(SILVER_DIR + SILVER_REVIEW_EP)

# Generate text_hash to create surrogate key for dim_review_text
review_df = review_df.withColumn(
    "review_hash",
    F.sha2("text", 256)
)
# dim_review_text table
dim_review_text = (
    review_df
    .select("text", "language","review_hash")
    .distinct()
)
## Utilize window function to create id for dim_review_text
window_spec = Window.orderBy("review_hash")
dim_review_text = dim_review_text.withColumn(
    "review_text_id",
    F.row_number().over(window_spec)
)

# Generate date to create surrogate key for dim_date
review_df = review_df.withColumn(
    "date",
    F.to_date("timestamp_created")
)
# dim_date table
dim_date = (
    review_df
    .select("appid","date")
    .withColumn("year", F.year("date")) 
    .withColumn("month", F.month("date")) 
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("quarter", F.quarter("date"))
    .distinct()
)
## Utilize window function to create id for dim_date
window_spec = Window.orderBy("date")
dim_date = dim_date.withColumn(
    "date_id",
    F.row_number().over(window_spec)
)


# bridge_game_dlc table
bridge_game_dlc = (
    content_df
    .select(
        F.col("appid").alias("game_id"),
        "dlc"
    )
    .distinct()
)
window_spec = Window.orderBy("game_id")
bridge_game_dlc = bridge_game_dlc.withColumn(
    "dlc_id",
    F.row_number().over(window_spec)
)


# bridge_game_production table
bridge_game_production = (
    content_df
    .select(
        F.col("appid").alias("game_id"),
        "publisher",
        "developer"
    )
    .distinct()
)
window_spec = Window.orderBy("game_id")
bridge_game_production = bridge_game_production.withColumn(
    "prod_id",
    F.row_number().over(window_spec)
)

# bridge_game_genre table
bridge_game_genre = (
    content_df
    .select(
        F.col("appid").alias("game_id"),
        "genre",
    )
    .distinct()
)
window_spec = Window.orderBy("game_id")
bridge_game_genre = bridge_game_genre.withColumn(
    "genre_id",
    F.row_number().over(window_spec)
)

# dim_game table
dim_game = (
    content_df.
    select(
        F.col("appid").alias("game_id"),
        "name",
        "type",
        "required_age",
        F.col("price_overview.original_price").alias("price"),
        F.col("price_overview.currency").alias("currency"),
        F.col("user_review.rating_value").alias("rating_value"),
        F.col("user_review.review_count").alias("review_count"),
        F.col("user_review.status").alias("status"),
        "release_date",
    )
)

# fact_review table

## Alias tables
r = review_df.alias("r")
dr = dim_review_text.alias("dr")
dd = dim_date.alias("dd")
dg = dim_game.alias("dg")

fact_review = (
    r
    # Join dim_review
    .join(
        dr.select("review_hash", "review_text_id"),
        F.col("r.review_hash") == F.col("dr.review_hash"),
        "left"
    )
    
    # Join dim_date
    .join(
        dd.select("appid", "date", "date_id"),
        (F.col("r.appid") == F.col("dd.appid")) &
        (F.col("r.date") == F.col("dd.date")),
        "left"
    )
    
    # Join dim_game
    .join(
        dg.select("game_id"),
        F.col("r.appid") == F.col("dg.game_id"),
        "left"
    )
    
    .select(
        F.col("dg.game_id"),
        F.col("dr.review_text_id"),
        F.col("dd.date_id"),
        F.col("r.recommended"),
        F.col("r.score")
    )
)
## Utilize window function to create id for fact_review
window_spec = Window.orderBy("game_id", "date_id", "review_text_id")
fact_review  = fact_review.withColumn(
    "review_id",
    F.row_number().over(window_spec)
)

# Cleaning after schema
dim_date = dim_date.drop("appid")


fact_review.printSchema()
dim_review_text.printSchema()
dim_date.printSchema()
dim_game.printSchema()

print(fact_review.count())
print(dim_review_text.count())
print(dim_date.count())
print(dim_game.count())