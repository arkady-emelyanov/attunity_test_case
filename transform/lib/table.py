from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType

from .constants import DATETIME_FORMAT
from .metadata import BatchMetadata


def process_special_fields(batch: BatchMetadata, df: DataFrame) -> DataFrame:
    # apply post load transformations (e.g. datetime)
    for col in batch.columns:
        if col['type'] == "DATETIME":
            src_field = col['name']
            tmp_field = f"{col['name']}_parsed"
            df.withColumn(tmp_field, to_timestamp(src_field, DATETIME_FORMAT)) \
                .drop(src_field) \
                .withColumnRenamed(tmp_field, src_field)
    return df


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
