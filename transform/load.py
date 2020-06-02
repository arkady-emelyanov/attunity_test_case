import argparse
import os.path

import sys
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType

from attunity.batch_metadata import get_batch_metadata
from attunity.constants import DATETIME_FORMAT
from attunity.mappings import get_schema_type
from helpers import get_spark

# 0. parse arguments
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("-l", "--load-path", required=True, help="Table load path")
arg_parser.add_argument("-d", "--delta-path", required=True, help="Delta table path")
cmd_args = arg_parser.parse_args()

# 1. list files, "load" type only
print(f">>> Searching for dfm in: {cmd_args.load_path}...")
dfm_files = []
for s in os.listdir(cmd_args.load_path):
    if s.startswith("LOAD") and s.endswith(".dfm"):
        dfm_files.append(os.path.join(cmd_args.load_path, s))

if not dfm_files:
    print(">>> Nothing to-do, exiting...")
    sys.exit(0)
else:
    print(f">>> Found {len(dfm_files)} load dfm files")

# 2. get batch and validate columns
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
    schema.add(col['name'], get_schema_type(col['type']))

# 4. produce initial parquet
print(f">>> Loading batch...")
spark = get_spark()
txt_files = spark.sparkContext.textFile(",".join(batch.files))
df = spark.read.json(txt_files, schema=schema)

# post-process fields
print(f">>> Post-processing columns...")
for col in batch.columns:
    if col['type'] == "DATETIME":
        src_field = col['name']
        tmp_field = f"{col['name']}_parsed"
        df.withColumn(tmp_field, to_timestamp(src_field, DATETIME_FORMAT)) \
            .drop(src_field) \
            .withColumnRenamed(tmp_field, src_field)

df.show(10, False)

# 5. creating a table
print(f">>> Writing delta table to: {cmd_args.delta_path}...")
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(cmd_args.delta_path)

# 6. done
print(">>> Done!")
