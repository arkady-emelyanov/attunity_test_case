from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def get_delta_table(
        spark: SparkSession,
        schema: StructType,
        delta_library_jar: str,
        delta_path: str):

    # load delta library jar, so we can use delta module
    spark.sparkContext.addPyFile(delta_library_jar)
    from delta.tables import DeltaTable

    # check existence of delta table
    if not DeltaTable.isDeltaTable(spark, delta_path):
        print(f">>> Delta table: {delta_path} is not initialized, performing initialization..")
        df = spark.createDataFrame([], schema=schema)
        df.write.format("delta").save(delta_path)

    return DeltaTable.forPath(spark, delta_path)
