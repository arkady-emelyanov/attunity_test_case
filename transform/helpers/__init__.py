from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    config = SparkConf() \
        .set("spark.sql.parquet.compression.codec", "gzip") \
        .set("spark.driver.memory", "4g") \
        .set("spark.executor.memory", "2g")

    spark = SparkSession.builder \
        .config(conf=config) \
        .getOrCreate()

    return spark
