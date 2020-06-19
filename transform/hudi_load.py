import sys

from lib.args import get_hudi_args
from lib.metadata import get_batch_metadata, get_metadata_file_list
from lib.spark import get_spark
from lib.table import process_special_fields

cmd_args = get_hudi_args()
spark = get_spark()

# list files, "load" type only
print(f">>> Searching for batch metadata files in: {cmd_args.load_path}...")
dfm_files = get_metadata_file_list(cmd_args.load_path, prefix="LOAD")
if not dfm_files:
    print(">>> Nothing to-do, exiting...")
    sys.exit(0)

# Get batch metadata and validate columns
print(f">>> Found {len(dfm_files)} batch metadata files, loading metadata...")
batch = get_batch_metadata(
    dfm_files=dfm_files,
    src_path_override=cmd_args.load_path
)
print(f">>> Metadata loaded, num_files={len(batch.files)}, records={batch.record_count}")
if not batch.files:
    raise Exception("Did not found any files to load..")

if len(batch.primary_key_columns) > 1:
    raise Exception("Composite primary keys not yet implemented!")

if len(batch.primary_key_columns) == 0:
    raise Exception("Batches without primary keys are not supported!")

# load batch
print(f">>> Loading batch...")
txt_files = spark.sparkContext.textFile(",".join(batch.files))
df = spark.read.json(txt_files, schema=batch.schema_batch)

# post-process fields
print(f">>> Post-processing columns...")
df = process_special_fields(batch, df)

# creating a table
print(f">>> Writing to Hudi {cmd_args.hudi_path}")
pkey = batch.primary_key_columns[0]

# https://hudi.apache.org/docs/configurations.html
hudi_options = {
    'hoodie.table.name': cmd_args.table_name,
    'hoodie.datasource.write.recordkey.field': pkey,
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.precombine.field': 'crdate',
    'hoodie.datasource.write.table.name': cmd_args.table_name,
    'hoodie.datasource.write.operation': 'bulkinsert',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

df.write \
    .format("hudi") \
    .options(**hudi_options) \
    .mode("overwrite") \
    .save(cmd_args.hudi_path)
