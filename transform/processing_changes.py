import os.path

import sys
from pyspark.sql.types import StructType

from helpers import get_spark
from attunity.attunity import get_batch
from attunity.mappings import get_type

SOURCE_PATH = "/Users/arkady/Projects/disney/spark_data/dbo.test_changing_load__ct"
OUTPUT_PATH = "/Users/arkady/Projects/disney/spark_data/out/dbo.test_changing_load"
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
batch = get_batch(
    dfm_files=dfm_files,
    src_path_override=SOURCE_PATH
)
print(f">>> Batch loaded, num_files={len(batch.files)}, records={batch.record_count}")

spark = get_spark()
print(spark)
