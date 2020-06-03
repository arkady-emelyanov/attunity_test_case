from lib.args import get_args
from lib.spark import get_spark

cmd_args = get_args()
spark = get_spark()

# Load delta table
print(f"Load delta table from {cmd_args.delta_path}...")
df = spark \
    .read \
    .format("delta") \
    .load(cmd_args.delta_path)

# Export to parquet snapshot
snapshot_partitions = 1
if cmd_args.snapshot_partitions:
    snapshot_partitions = int(cmd_args.snapshot_partitions)

print(f"Storing snapshot: {cmd_args.snapshot_path}, with {snapshot_partitions} partitions...")
df.repartition(snapshot_partitions) \
    .write \
    .mode("overwrite") \
    .parquet(cmd_args.snapshot_path)

# 3. done
print("Done!")
