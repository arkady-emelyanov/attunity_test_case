from argparse import Namespace

from pyspark.sql import SparkSession
from pyspark.sql.functions import max as sql_max

from .lib.constants import CHANGES_METADATA_OPERATION, CHANGES_METADATA_TIMESTAMP
from .lib.metadata import get_batch_metadata, get_metadata_file_list
from .lib.table import get_delta_table, process_special_fields


def apply_changes_task(spark: SparkSession, cmd_args: Namespace):
    # List "change" files
    print(f">>> Searching for batch metadata files in: {cmd_args.changes_path}...")
    dfm_files = get_metadata_file_list(cmd_args.changes_path)
    if not dfm_files:
        print(">>> Nothing to-do, exiting...")
        return

    # Get batch metadata and validate columns
    print(f">>> Found {len(dfm_files)} batch metadata files, loading metadata...")
    batch = get_batch_metadata(
        dfm_files=dfm_files,
        src_path_override=cmd_args.changes_path
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
    batch_df = batch_df \
        .filter(batch_df[CHANGES_METADATA_OPERATION].isin(["I", "U", "D"])) \
        .orderBy(batch_df[CHANGES_METADATA_TIMESTAMP].asc())
    print(f">>> Collected: {batch_df.count()} changes after operation filtering...")

    # Transform Qlik changes into DeltaLake expected changes
    print(f">>> Transforming collected changes into DeltaLake compatible DataFrame...")
    if len(batch.primary_key_columns) > 1:
        raise Exception("Composite primary keys not yet implemented!")

    if len(batch.primary_key_columns) == 0:
        raise Exception("Batches without primary keys are not supported!")

    # Translate changes
    pkey = batch.primary_key_columns[0]
    cols = ",\n".join(batch.columns_without_pkey())
    payload_cols = f'''
        struct(
            header__timestamp,
            CASE WHEN header__change_oper = 'D' THEN true ELSE false END as deleted,
            {cols}
        ) as payload_cols
    '''
    latest_changes_df = batch_df \
        .selectExpr(pkey, payload_cols) \
        .groupBy(pkey) \
        .agg(sql_max("payload_cols").alias("latest")) \
        .selectExpr(pkey, "latest.*") \
        .drop(*batch.metadata_columns)
    print(f">>> Collected: {latest_changes_df.count()} after changes compaction...")

    # Load delta table
    print(f">>> Loading delta table from {cmd_args.delta_path}...")
    delta_table = get_delta_table(
        spark=spark,
        schema=batch.schema_table,
        delta_library_jar=cmd_args.delta_library_jar,
        delta_path=cmd_args.delta_path,
    )

    # grab only meaning columns, skip the rest
    value_map = {}
    for col in batch.columns:
        dst = col['name']
        if dst not in batch.metadata_columns:
            src = f"s.{dst}"
            value_map[dst] = src

    # Apply changes
    # @see: https://docs.delta.io/latest/delta-update.html#write-change-data-into-a-delta-table
    print(f">>> Applying changes to target delta table...")
    delta_table \
        .alias("t") \
        .merge(latest_changes_df.alias("s"), f"s.{pkey} = t.{pkey}") \
        .whenMatchedDelete(condition="s.deleted = true") \
        .whenMatchedUpdate(set=value_map) \
        .whenNotMatchedInsert("s.deleted = false", values=value_map) \
        .execute()

    print(">>> Done!")
