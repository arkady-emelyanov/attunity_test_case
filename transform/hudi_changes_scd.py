import sys

from lib.args import get_hudi_args
from lib.constants import CHANGES_METADATA_OPERATION, CHANGES_METADATA_TIMESTAMP
from lib.metadata import get_batch_metadata, get_metadata_file_list
from lib.spark import get_spark
from lib.table import process_special_fields
from lib.hudi_helpers import get_operation_options

cmd_args = get_hudi_args()
spark = get_spark()

# List "change" files
print(f">>> Searching for batch metadata files in: {cmd_args.load_path}...")
dfm_files = get_metadata_file_list(cmd_args.load_path)
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

# Load batch
print(f">>> Loading batch...")
txt_files = spark.sparkContext.textFile(",".join(batch.files))
batch_df = spark.read.json(txt_files, schema=batch.schema_batch)
print(f">>> Collected: {batch_df.count()} changes before operation filtering...")

# Post-process fields and filter out
print(f">>> Post-processing columns...")
batch_df = process_special_fields(batch, batch_df)
batch_df.show(20)

