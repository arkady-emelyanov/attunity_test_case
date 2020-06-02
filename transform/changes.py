import os.path
import sys
from pyspark.sql.types import StructType

from lib.constants import CHANGES_METADATA_FIELD_PREFIX
from lib.mappings import get_schema_type
from lib.batch_metadata import get_batch_metadata
from helpers import get_spark

# 0. parse arguments

# Load batch
# Collect changes as separate DataFrames: insert, delete and update
# Load delta table
# Apply delta changes: https://docs.delta.io/0.4.0/delta-update.html#write-change-data-into-a-delta-table
