import os.path

import sys
from pyspark.sql.types import StructType

from helpers import get_spark
from attunity.batch_metadata import get_batch_metadata
from attunity.mappings import get_schema_type

# Load batch
# Collect changes as separate DataFrames: insert, delete and update
# Load delta table
# Apply delta changes: https://docs.delta.io/0.4.0/delta-update.html#write-change-data-into-a-delta-table

SOURCE_PATH = "/Users/arkady/Projects/disney/spark_data/dbo.WRKFLW_INSTNC__ct"
DELTA_TABLE = "/Users/arkady/Projects/disney/spark_data/out/WRKFLW_INSTNC"
PREFIX_SKIP = "header__"

# 1. list files, "load" type only
print(">>> Searching for load dfm files...")
dfm_files = []
for s in os.listdir(SOURCE_PATH):
    if s.endswith(".dfm"):
        dfm_files.append(os.path.join(SOURCE_PATH, s))

if not dfm_files:
    print(">>> Nothing to-do, exiting...")
    sys.exit(0)

# 2. get batch and validate columns
batch = get_batch_metadata(
    dfm_files=dfm_files,
    src_path_override=SOURCE_PATH
)
print(f">>> Batch metadata loaded, num_files={len(batch.files)}, records={batch.record_count}")
if not batch.files:
    raise Exception("Did not found any files to load..")

# 3. define schema

# 4. divide updates/deletes into different data frames

# 5. apply changes to delta table
spark = get_spark()
print(spark)
