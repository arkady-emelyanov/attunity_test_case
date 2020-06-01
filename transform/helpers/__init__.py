from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    config = SparkConf() \
        .set("spark.sql.parquet.compression.codec", "gzip")

    spark = SparkSession.builder \
        .config(conf=config) \
        .getOrCreate()

    return spark
