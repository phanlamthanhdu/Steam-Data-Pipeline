from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import boto3
import yaml


def get_spark_session(app_name, master="yarn"):
    """
    Get a Spark Session

    Parameters:
        app_name : str
            Pass the name of your app
        master : str
            Choosing the Spark master, yarn is the default
    Returns:
        spark: SparkSession
    """
    spark = (SparkSession
             .builder
             .appName(app_name)
             .master(master=master)
             .getOrCreate())

    return spark

def read_s3_file(spark, prefix, endpoint, file):
    pass

def get_s3_client():
    pass