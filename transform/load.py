import sys

from lib.args import get_args
from lib.metadata import get_batch_metadata, get_metadata_file_list
from lib.spark import get_spark
from lib.table import process_special_fields

cmd_args = get_args()
spark = get_spark()

# list files, "load" type only
print(f">>> Searching for batch metadata files in: {cmd_args.load_path}...")
dfm_files = get_metadata_file_list(cmd_args.load_path, prefix="LOAD")
if not dfm_files:
    print(">>> Nothing to-do, exiting...")
    sys.exit(0)

# get batch and validate columns
print(f">>> Found {len(dfm_files)} batch metadata files, loading metadata...")
batch = get_batch_metadata(
    dfm_files=dfm_files,
    src_path_override=cmd_args.load_path
)
print(f">>> Metadata loaded, num_files={len(batch.files)}, records={batch.record_count}")
if not batch.files:
    raise Exception("Did not found any files to load..")

# load batch
print(f">>> Loading batch...")
txt_files = spark.sparkContext.textFile(",".join(batch.files))
df = spark.read.json(txt_files, schema=batch.schema_batch)

# post-process fields
print(f">>> Post-processing columns...")
df = process_special_fields(batch, df)

# TODO: calculate number of partitions based on dataset size
partitions = 1

# creating a table
print(f">>> Writing initial delta table to: {cmd_args.delta_path}...")
df.repartition(partitions) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(cmd_args.delta_path)

# done
print(">>> Done!")
