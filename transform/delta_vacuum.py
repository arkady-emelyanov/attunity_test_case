from lib.args import get_args
from lib.spark import get_spark
from lib.table import calculate_partitions, get_delta_table

cmd_args = get_args()
spark = get_spark()

# Load
print(f">>> Load delta table from {cmd_args.delta_path}...")
df = spark \
    .read \
    .format("delta") \
    .load(cmd_args.delta_path)

# Compact
print(f">>> Compacting delta table...")
partitions = calculate_partitions(spark=spark, df=df)
df.repartition(partitions). \
    write \
    .option("dataChange", "false") \
    .format("delta") \
    .mode("overwrite") \
    .save(cmd_args.delta_path)

# Vacuum
print(f">>> Vacuuming delta table...")
delta_table = get_delta_table(
    spark=spark,
    schema=df.schema,
    delta_library_jar=cmd_args.delta_library_jar,
    delta_path=cmd_args.delta_path,
)
delta_table.vacuum(0.000001)
print(">>> Done!")
