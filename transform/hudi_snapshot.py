from lib.args import get_hudi_args
from lib.spark import get_spark
from lib.table import calculate_partitions
from lib.constants import CHANGES_METADATA_FIELD_PREFIX

cmd_args = get_hudi_args()
spark = get_spark()

# Load Hudi table
print(f">>> Load Hudi table from {cmd_args.hudi_path}...")
df = spark \
    .read \
    .format("hudi") \
    .load(f"{cmd_args.hudi_path}/*")

# Filter Qlik metadata fields
df = df \
    .select([c for c in df.columns if not c.startswith(CHANGES_METADATA_FIELD_PREFIX)])

# Export to parquet snapshot
partitions = calculate_partitions(spark=spark, df=df)
print(f">>> Storing snapshot: {cmd_args.snapshot_path}, with {partitions} partition(s)...")

df.repartition(partitions) \
    .write \
    .mode("overwrite") \
    .parquet(cmd_args.snapshot_path)

print(">>> Done!")
