import sys
from pyspark.sql.types import StructType

from lib.constants import CHANGES_METADATA_FIELD_PREFIX, CHANGES_METADATA_OPERATION
from lib.helpers import get_spark, get_args
from lib.mappings import get_schema_type
from lib.metadata import get_batch_metadata, get_metadata_file_list
from lib.table import get_delta_table

# 0. parse arguments
cmd_args = get_args()

# 1. list "change" files
print(f">>> Searching for batch metadata files in: {cmd_args.changes_path}...")
dfm_files = get_metadata_file_list(cmd_args.changes_path)
if not dfm_files:
    print(">>> Nothing to-do, exiting...")
    sys.exit(0)

# 2. get batch and validate columns
print(f">>> Found {len(dfm_files)} batch metadata files, loading metadata...")
batch = get_batch_metadata(
    dfm_files=dfm_files,
    src_path_override=cmd_args.changes_path
)
print(f">>> Metadata loaded, num_files={len(batch.files)}, records={batch.record_count}")
if not batch.files:
    raise Exception("Did not found any files to load..")

# 3. define schema
print(">>> Setting up DataFrame schema...")
schema = StructType()
metadata_columns = []

for col in batch.columns:
    schema.add(col['name'], get_schema_type(col['type']))
    if col['name'].startswith(CHANGES_METADATA_FIELD_PREFIX):
        metadata_columns.append(col['name'])

# 4. load batch
print(f">>> Loading batch and filter out no-op CDC events...")
spark = get_spark()
txt_files = spark.sparkContext.textFile(",".join(batch.files))
batch_df = spark.read.json(txt_files, schema=schema)
batch_df = batch_df.filter(
    (batch_df[CHANGES_METADATA_OPERATION] == "U") |
    (batch_df[CHANGES_METADATA_OPERATION] == "I") |
    (batch_df[CHANGES_METADATA_OPERATION] == "D")
)

# TODO: sort by header__timestamp
print(f">>> Collected {batch_df.count()} distinct changes")
batch_df.show(10, False)

# 5. Transform Qlik changes into DeltaLake expected changes
print(f">>> Transforming collected changes into DeltaLake compatible DataFrame...")
# TODO: drop header__ fields

# 6. Load delta table
# TODO: check delta table existence, if doesn't exist, create new
print(f">>> Loading delta table from {cmd_args.delta_path}...")
delta_table = get_delta_table(spark, cmd_args.delta_path)

# Apply delta changes
# @see: https://docs.delta.io/0.4.0/delta-update.html#write-change-data-into-a-delta-table
print(">>> Done!")
