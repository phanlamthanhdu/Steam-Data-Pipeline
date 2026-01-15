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
        StructField("appid",IntegerType(), True),
        StructField("name",StringType(), True),
        StructField("type",StringType(), True),
        StructField("require_age", StringType(), True),         # not number cause it's the age tag
        StructField("dlc_list",ArrayType(IntegerType()),True),
        StructField(
            "price_overview",
            StructType([
                StructField("currency",StringType(),True),
                StructField("original_price",DoubleType(),True)
            ]),
            True
        ),
        StructField("description",StringType(),True),
        StructField("pc_requirements",StringType(),True),
        StructField(
            "user_review",
            StructType([
                StructField("reviewCount",IntegerType(),True),
                StructField("ratingValue",IntegerType(),True),
                StructField("bestRating",IntegerType(),True),
                StructField("worstRating",IntegerType(),True),
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
        StructField("appid",IntegerType(),True),
        StructField("reviews",ArrayType(StringType()),True)    
    ]),
    "app_tag" : StructType([
        StructField("tagid",IntegerType(),True),
        StructField("name",StringType(),True)
    ])
}