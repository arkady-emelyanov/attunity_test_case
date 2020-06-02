import sys
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType

from lib.constants import DATETIME_FORMAT
from lib.helpers import get_spark, get_args
from lib.mappings import get_schema_type
from lib.metadata import get_batch_metadata, get_metadata_file_list

cmd_args = get_args()

# 1. list files, "load" type only
print(f">>> Searching for batch metadata files in: {cmd_args.load_path}...")
dfm_files = get_metadata_file_list(cmd_args.load_path, prefix="LOAD")
if not dfm_files:
    print(">>> Nothing to-do, exiting...")
    sys.exit(0)

# 2. get batch and validate columns
print(f">>> Found {len(dfm_files)} batch metadata files, loading metadata...")
batch = get_batch_metadata(
    dfm_files=dfm_files,
    src_path_override=cmd_args.load_path
)
print(f">>> Metadata loaded, num_files={len(batch.files)}, records={batch.record_count}")
if not batch.files:
    raise Exception("Did not found any files to load..")

# 3. define schema
print(">>> Setting up DataFrame schema...")
schema = StructType()
for col in batch.columns:
    is_required = int(col['primaryKeyPos']) > 0
    schema.add(col['name'], get_schema_type(col['type']), is_required)

# 4. load batch
print(f">>> Loading batch...")
spark = get_spark()
txt_files = spark.sparkContext.textFile(",".join(batch.files))
df = spark.read.json(txt_files, schema=schema)

# 5. post-process fields
print(f">>> Post-processing columns...")
for col in batch.columns:
    if col['type'] == "DATETIME":
        src_field = col['name']
        tmp_field = f"{col['name']}_parsed"
        df.withColumn(tmp_field, to_timestamp(src_field, DATETIME_FORMAT)) \
            .drop(src_field) \
            .withColumnRenamed(tmp_field, src_field)

df.show(10)

# 6. creating a table
print(f">>> Writing delta table to: {cmd_args.delta_path}...")
df.write \
    .format("delta") \
    .save(cmd_args.delta_path)

# 7. done
print(">>> Done!")
