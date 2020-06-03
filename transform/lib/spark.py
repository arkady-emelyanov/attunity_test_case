from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    conf = SparkConf() \
        .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    return SparkSession. \
        builder \
        .config(conf=conf) \
        .getOrCreate()
