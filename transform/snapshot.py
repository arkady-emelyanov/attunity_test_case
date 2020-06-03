from lib.args import get_args
from lib.spark import get_spark

cmd_args = get_args()
spark = get_spark()

# Load delta table
print(f">>> Load delta table from {cmd_args.delta_path}...")
df = spark \
    .read \
    .format("delta") \
    .load(cmd_args.delta_path)


# Export to parquet snapshot
# TODO: calculate number of partitions based on dataset size
partitions = 1

print(f">>> Storing snapshot: {cmd_args.snapshot_path}, with {partitions} partitions...")
df.repartition(partitions) \
    .write \
    .mode("overwrite") \
    .parquet(cmd_args.snapshot_path)
print(">>> Done!")
