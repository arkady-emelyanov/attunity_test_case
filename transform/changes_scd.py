import sys
from pyspark.sql.functions import col, lit
from pyspark.sql.types import TimestampType, BooleanType, StringType

from lib.args import get_args
from lib.constants import CHANGES_METADATA_OPERATION, CHANGES_METADATA_TIMESTAMP
from lib.metadata import get_batch_metadata, get_metadata_file_list
from lib.spark import get_spark
from lib.table import process_special_fields, get_delta_table

cmd_args = get_args()
spark = get_spark()

# List "change" files
print(f">>> Searching for batch metadata files in: {cmd_args.changes_path}...")
dfm_files = get_metadata_file_list(cmd_args.changes_path)
if not dfm_files:
    print(">>> Nothing to-do, exiting...")
    sys.exit(0)

# Get batch metadata and validate columns
print(f">>> Found {len(dfm_files)} batch metadata files, loading metadata...")
batch = get_batch_metadata(
    dfm_files=dfm_files,
    src_path_override=cmd_args.changes_path
)
print(f">>> Metadata loaded, num_files={len(batch.files)}, records={batch.record_count}")
if not batch.files:
    raise Exception("Did not found any files to load..")

if len(batch.primary_key_columns) > 1:
    raise Exception("Composite primary keys not yet implemented!")

if len(batch.primary_key_columns) == 0:
    raise Exception("Batches without primary keys are not supported!")

# Load batch
print(f">>> Loading batch...")
txt_files = spark.sparkContext.textFile(",".join(batch.files))
batch_df = spark.read.json(txt_files, schema=batch.schema_batch)
print(f">>> Collected: {batch_df.count()} changes before operation filtering...")

# Post-process fields and filter out
print(f">>> Collecting INSERT and UPDATE operations...")
batch_df = process_special_fields(batch, batch_df)
primary_key = batch.primary_key_columns[0]

inserts_df = batch_df \
    .filter(batch_df[CHANGES_METADATA_OPERATION].isin(["I"])) \
    .orderBy(batch_df[CHANGES_METADATA_TIMESTAMP].asc()) \
    .withColumn("_merge_key", lit(None).cast(StringType()))

updates_df = batch_df \
    .filter(batch_df[CHANGES_METADATA_OPERATION].isin(["U"])) \
    .orderBy(batch_df[CHANGES_METADATA_TIMESTAMP].asc()) \
    .withColumn("_merge_key", col(primary_key))

# TODO: updates must be turned into inserts as well

print(f">>> Collected: UPDATES={updates_df.count()}, INSERTS={inserts_df.count()}")

scd_schema = batch.schema_table
scd_schema.add('current', BooleanType(), False)
scd_schema.add('effective_date', TimestampType(), True)
scd_schema.add('end_date', TimestampType(), True)

inserts_df.show(20, False)
updates_df.show(20, False)

# Load delta table
print(f">>> Loading SDC Type 2 delta table from {cmd_args.delta_sdc_path}...")
scd_delta_table = get_delta_table(
    spark=spark,
    schema=batch.schema_table,
    delta_library_jar=cmd_args.delta_library_jar,
    delta_path=cmd_args.delta_sdc_path,
)

# grab only meaning columns, skip the rest
value_map = {}
for col in batch.columns:
    dst = col['name']
    if dst not in batch.metadata_columns:
        src = f"s.{dst}"
        value_map[dst] = src

value_map.update({
    'current': "true",
    'effective_date': "s.header__timestamp",
    'end_date': "null",
})

# Apply changes
# @see: https://docs.databricks.com/delta/delta-update.html#scd-type-2-using-merge-notebook
print(f">>> Applying changes to target delta table...")
scd_delta_table \
    .alias("t") \
    .merge(inserts_df.alias("s"), f"t.{primary_key} = _merge_key") \
    .whenNotMatchedInsert(values=value_map) \
    .execute()

scd_delta_table \
    .alias("t") \
    .merge(updates_df.alias("s"), f"t.{primary_key} = _merge_key") \
    .whenMatchedUpdate(condition="t.current = true",
                       set={
                           "current": "false",
                           "end_date": "s.header__timestamp"
                       }) \
    .execute()

scd_delta_table \
    .alias("t") \
    .merge(updates_df.alias("s"), "1 = 0") \
    .whenNotMatchedInsert(values=value_map) \
    .execute()

# TODO: soft delete


# temp
scd_delta_table \
    .toDF() \
    .show(20, False)

print(">>> Done!")
