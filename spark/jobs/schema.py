from pyspark.sql.types import (IntegerType,
                               StringType,
                               DoubleType,
                               StructField,
                               StructType,
                               LongType,
                               BooleanType,
                               ArrayType                               
)

schema = {
    "app_content" : StructType([
        StructField("appid",StringType(), True),
        StructField("name",StringType(), True),
        StructField("type",StringType(), True),
        StructField("required_age", StringType(), True),         # not number cause it's the age tag
        StructField("dlc_list",ArrayType(StringType()),True),
        StructField(
            "price_overview",
            StructType([
                StructField("currency",StringType(),True),
                StructField("original_price",StringType(),True)
            ]),
            True
        ),
        StructField("about_the_game",StringType(),True),
        StructField("pc_requirements",StringType(),True),
        StructField(
            "user_review",
            StructType([
                StructField("reviewCount",StringType(),True),
                StructField("ratingValue",StringType(),True),
                StructField("bestRating",StringType(),True),
                StructField("worstRating",StringType(),True),
                StructField("status",StringType(),True)    
            ]),
            True
        ),
        StructField("release_date",StringType(),True),
        StructField("publishers",ArrayType(StringType()),True),
        StructField("developers",ArrayType(StringType()),True),
        StructField("genres",ArrayType(StringType()),True), 
        StructField("user_tags",ArrayType(StringType()),True)
    ]),
    "app_review" : StructType([
        StructField("appid",StringType(),True),
        StructField("reviews",
            ArrayType(
                StructType([
                    StructField("recommendationid", StringType()),
                    StructField("author", StructType([
                        StructField("steamid", StringType()),
                        StructField("num_games_owned", StringType()),
                        StructField("num_reviews", StringType()),
                        StructField("playtime_forever", StringType()),
                        StructField("playtime_last_two_weeks", StringType()),
                        StructField("playtime_at_review", StringType()),
                        StructField("last_played", StringType()),
                    ])),
                    StructField("language", StringType()),
                    StructField("review", StringType()),
                    StructField("timestamp_created", StringType()),
                    StructField("timestamp_updated", StringType()),
                    StructField("voted_up", StringType()),
                    StructField("votes_up", StringType()),
                    StructField("votes_funny", StringType()),
                    StructField("weighted_vote_score", StringType()),
                    StructField("comment_count", StringType()),
                    StructField("steam_purchase", StringType()),
                    StructField("received_for_free", StringType()),
                    StructField("written_during_early_access", StringType()),
                    StructField("primarily_steam_deck", StringType())
                ])
            )
        ,True)    
    ]),
    "app_tag" : StructType([
        StructField("tagid",StringType(),True),
        StructField("name",StringType(),True)
    ])
}