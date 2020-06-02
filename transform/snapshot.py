import argparse

from lib.helpers import get_spark, get_args

cmd_args = get_args()
spark = get_spark()

# 1. load delta table
print(f"Load delta table from {cmd_args.delta_path}...")
df = spark.read.format("delta").load(cmd_args.delta_path)
df.show(10, False)

# 2. export to parquet
print(f"Storing snapshot in {cmd_args.snapshot_path}...")
df.write.mode("overwrite").parquet(cmd_args.snapshot_path)
print("Done!")
