from argparse import Namespace

from pyspark.sql import SparkSession

from .lib.table import calculate_partitions


def snapshot_task(spark: SparkSession, cmd_args: Namespace):
    # Load delta table
    print(f">>> Load delta table from {cmd_args.delta_path}...")
    df = spark \
        .read \
        .format("delta") \
        .load(cmd_args.delta_path)

    # Export to parquet snapshot
    partitions = calculate_partitions(spark=spark, df=df)
    print(f">>> Storing snapshot: {cmd_args.snapshot_path}, with {partitions} partitions...")

    df.repartition(partitions) \
        .write \
        .mode("overwrite") \
        .parquet(cmd_args.snapshot_path)
    print(">>> Done!")
