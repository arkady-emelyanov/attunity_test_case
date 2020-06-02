from pyspark.sql import SparkSession

from .args import get_args


def get_delta_table(spark: SparkSession, delta_path: str):
    cmd_args = get_args()

    # load delta library jar
    spark.sparkContext \
        .addPyFile(cmd_args.delta_library_jar)

    from delta.tables import DeltaTable
    return DeltaTable.forPath(spark, delta_path)
