from schema import schema
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, BooleanType


SILVER_DIR = "s3a://steam-datalake-duplt/silver/"
CONTENT_DIR = "s3a://steam-datalake-duplt/raw/contents/app_content.txt"
REVIEW_DIR = "s3a://steam-datalake-duplt/raw/reviews/app_review.txt"

spark = (SparkSession
        .builder
        .appName("ContentProcess")
        .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .getOrCreate()
)

content_df = (
        spark.read
        .schema(schema["app_content"])
        .json(CONTENT_DIR)
)

review_df = (
        spark.read
        .schema(schema["app_review"])
        .json(REVIEW_DIR)
)

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.parser.locale", "en-US")

# Cleaning & Normalization
content_df = (
        content_df
        .withColumn("appid", F.col("appid").cast(IntegerType()))
        .withColumn("dlc_list", F.transform("dlc_list", lambda x: x.cast("int")))
        .withColumn(
                "price_overview",
                F.struct(
                        F.col("price_overview.currency").alias("currency"),
                        F.when(F.col("price_overview.original_price") == "Free", 0.0)
                        .when(
                        (F.col("price_overview.original_price").isNull()) |
                        (F.lower(F.col("price_overview.original_price")).isin("none", "null")) |
                        (F.col("price_overview.original_price").contains("₫")),
                        None
                        )
                        .otherwise(
                        F.col("price_overview.original_price").cast("double")
                        )
                        .alias("original_price")
                )
        )
        .withColumn(
                "user_review",
                F.when(F.col("user_review").isNull(),None)
                .otherwise(
                        F.struct(
                                F.col("user_review.reviewCount").cast(IntegerType()).alias("review_count"),
                                F.col("user_review.ratingValue").cast(DoubleType()).alias("rating_value"),
                                F.col("user_review.bestRating").cast(IntegerType()).alias("best_rating"),
                                F.col("user_review.worstRating").cast(IntegerType()).alias("worst_rating"),
                                F.when(F.col("user_review.status").rlike("[0-9]"), None)
                                .when(F.lower(F.col("user_review.status")).contains("positive"),"Positive")
                                .when(F.lower(F.col("user_review.status")).contains("negative"),"Negative")
                                .otherwise(F.col("user_review.status")).alias("status")
                        )       
                )
        )
        .withColumn(
                "release_date",
                F.when(F.col("release_date") == "Coming Soon", None)
                .otherwise(
                        F.to_date(
                                F.coalesce(
                                        F.to_timestamp("release_date", "MMM d, yyyy"),
                                        F.to_timestamp("release_date", "d MMM, yyyy")
                                )
                        )
                )  # yyyy-mm-dd
        )
        .drop("pc_requirements","about_the_game")
)

review_df = (
        review_df
        .withColumn("appid", F.col("appid").cast(IntegerType()))
        .filter(F.size("reviews") > 0)
        .withColumn(
                "reviews",
                F.arrays_zip(
                        F.col("reviews.review"),
                        F.col("reviews.voted_up"),
                        F.col("reviews.language"),
                        F.col("reviews.timestamp_created"),
                        F.col("reviews.weighted_vote_score")
                )
        )
)

# Exploding columns
# content_df = (
#         content_df 
#         .withColumn("dlc", F.explode("dlc_list"))
#         .withColumn("publisher", F.explode("publishers"))
#         .withColumn("developer", F.explode("developers"))
#         .withColumn("genre", F.explode("genres"))
#         .drop("publishers","developers","genres","user_tags","pc_requirements","about_the_game")
#         .dropna()
# )
review_df = (
        review_df
        .withColumn("reviews", F.explode("reviews"))
        .select(
                F.col("appid"),
                F.col("reviews.review").alias("text"),
                F.col("reviews.voted_up").cast(BooleanType()).alias("recommended"),
                F.col("reviews.language").alias("language"),
                F.col("reviews.timestamp_created").alias("timestamp_created"),
                F.col("reviews.weighted_vote_score").alias("score")
        )
        .dropna()
)

review_df.write.mode("overwrite").parquet(SILVER_DIR + "stg_review")
content_df.write.mode("overwrite").parquet(SILVER_DIR + "stg_content")

spark.stop()