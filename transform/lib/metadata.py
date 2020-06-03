#
# Set of Attunity helpers
#
import json
import os

from pyspark.sql.types import *
from typing import List

from .constants import CHANGES_METADATA_FIELD_PREFIX

# SQL Server schema mappings
TYPE_MAPPINGS = {
    "BOOLEAN": BooleanType,
    "INT4": IntegerType,
    "INT8": LongType,
    "DATETIME": StringType,
    "STRING": StringType,
    "WSTRING": StringType,
    "BYTES": StringType,
    "NCLOB": StringType,
}


def get_schema_type(source_type: str) -> DataType:
    if source_type not in TYPE_MAPPINGS:
        raise Exception(f"Unknown mapping '{source_type}'")
    return TYPE_MAPPINGS.get(source_type)()


class BatchMetadata:
    def __init__(self, columns=None, files=None, record_count=0):
        self.columns = columns
        self.files = files
        self.record_count = record_count
        self.primary_key_columns = []

        # schema related fields
        self.schema_batch = StructType()
        self.schema_table = StructType()
        self.metadata_columns = []

        # initialize
        self._set_primary_key_columns()
        self._generate_schema()

    def _set_primary_key_columns(self):
        for col in self.columns:
            primary_key_pos = int(col['primaryKeyPos'])
            if primary_key_pos > 0:
                self.primary_key_columns.append(col)
        self.primary_key_columns.sort(key=lambda x: int(col['primaryKeyPos']))
        self.primary_key_columns = [x['name'] for x in self.primary_key_columns]

    def _generate_schema(self):
        for col in self.columns:
            col_type = get_schema_type(col['type'])
            col_name = col['name']
            is_nullable = int(col['primaryKeyPos']) > 0

            self.schema_batch.add(
                col_name,
                col_type,
                nullable=is_nullable
            )
            if col['name'].startswith(CHANGES_METADATA_FIELD_PREFIX):
                self.metadata_columns.append(col['name'])
            else:
                self.schema_table.add(
                    col_name,
                    col_type,
                    nullable=is_nullable
                )

    def columns_without_pkey(self) -> List[str]:
        res = []
        for col in self.columns:
            name = col['name']
            if name not in self.metadata_columns and name not in self.primary_key_columns:
                res.append(name)
        return res


def get_metadata_file_list(search_path: str, prefix: str = "") -> List[str]:
    dfm_files = []
    suffix = ".dfm"
    for s in os.listdir(search_path):
        if prefix and not s.startswith(prefix):
            print(f"> Prefix is set: {prefix}, but {s} doesn't contain it, skipping...")
            continue
        if suffix and not s.endswith(suffix):
            continue
        dfm_files.append(os.path.join(search_path, s))
    return dfm_files


def get_batch_metadata(dfm_files: List[str], src_path_override: str) -> BatchMetadata:
    df_columns = None
    df_files = []
    df_record_count = 0

    for dfm_file in dfm_files:
        with open(dfm_file, 'r') as f:
            obj = json.loads(f.read())

        # validate loaded object
        if type(obj) != dict:
            raise Exception(f"Not a JSON in: {dfm_file}")

        if obj['dfmVersion'] != "1.1":
            raise Exception("Unknown dfmVersion")

        if obj['formatInfo']["format"] != "json":
            raise Exception(f"Unknown format in {dfm_file}")

        # get columns
        columns = obj['dataInfo']['columns']
        if df_columns is None:
            df_columns = columns

        # validate columns match per each batch
        if columns != df_columns:
            raise Exception(f"Different number of columns in {dfm_file}")

        # adjust record count
        df_record_count += int(obj['fileInfo']['recordCount'])

        # construct final file name
        dfm_file = ".".join([
            obj['fileInfo']['name'],
            obj['fileInfo']['extension']
        ])

        # override source path if working with
        # locally stored data
        if src_path_override:
            file_full_path = os.path.join(src_path_override, dfm_file)
        else:
            # TODO: append s3 prefix here
            file_full_path = obj['fileInfo']['location']

        df_files.append(file_full_path)

    return BatchMetadata(
        columns=df_columns,
        files=df_files,
        record_count=df_record_count,
    )
