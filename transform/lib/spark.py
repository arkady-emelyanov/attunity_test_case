from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    return SparkSession. \
        builder \
        .config(conf=SparkConf()) \
        .getOrCreate()
