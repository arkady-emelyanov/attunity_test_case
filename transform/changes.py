import sys
from pyspark.sql.functions import max as sql_max
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
).orderBy(batch_df['header__timestamp'].asc())

print(f">>> Collected {batch_df.count()} changes before compaction")

# 5. Transform Qlik changes into DeltaLake expected changes
print(f">>> Transforming collected changes into DeltaLake compatible DataFrame...")
if len(batch.primary_key_columns) > 1:
    raise Exception("Composite primary keys not yet implemented")

if len(batch.primary_key_columns) == 0:
    raise Exception("Batches without primary keys not supported")

# translate changes
pkey = batch.primary_key_columns[0]['name']
cols = ",\n".join([
    x['name'] for x in batch.columns if (int(x['primaryKeyPos']) == 0 and x['name'] not in metadata_columns)
])
payload_cols = f'''
    struct(
        header__timestamp,
        CASE WHEN header__change_oper = 'U' THEN true ELSE false END as updated,
        CASE WHEN header__change_oper = 'D' THEN true ELSE false END as deleted,
        {cols}
    ) as payload_cols
'''
latest_changes_df = batch_df \
    .selectExpr(pkey, payload_cols) \
    .groupBy(pkey) \
    .agg(sql_max("payload_cols").alias("latest")) \
    .selectExpr(pkey, "latest.*") \
    .drop(*metadata_columns)

print(f">>> Collected {latest_changes_df.count()} changes after compaction")
latest_changes_df.show(10)

# 6. Load delta table
print(f">>> Loading delta table from {cmd_args.delta_path}...")
delta_table = get_delta_table(spark, cmd_args.delta_path)

# 7. Apply changes
# @see: https://docs.delta.io/latest/delta-update.html#write-change-data-into-a-delta-table
print(f">>> Applying changes to target delta table...")
value_map = {}
for col in batch.columns:
    dst = col['name']
    if dst not in metadata_columns:
        src = f"s.{dst}"
        value_map[dst] = src

delta_table \
    .alias("t") \
    .merge(latest_changes_df.alias("s"), f"s.{pkey} = t.{pkey}") \
    .whenMatchedDelete(condition="s.deleted = true") \
    .whenMatchedUpdate(condition="s.updated = true", set=value_map) \
    .whenNotMatchedInsert("s.deleted = false and s.updated = false", values=value_map) \
    .execute()

# Wow.
print(">>> Done!")
