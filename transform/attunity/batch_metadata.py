#
# Set of Attunity helpers
#
import json
import os

from typing import List


class BatchMetadata:
    def __init__(self, columns=None, files=None, record_count=0):
        self.columns = columns
        self.files = files
        self.record_count = record_count
        self.primary_key_columns = []
        self._set_primary_key_columns()

    def _set_primary_key_columns(self):
        for col in self.columns:
            primary_key_pos = int(col['primaryKeyPos'])
            if primary_key_pos > 0:
                self.primary_key_columns.append(col)
        self.primary_key_columns.sort(key=lambda x: int(col['primaryKeyPos']))


def get_batch_metadata(dfm_files: List[str], src_path_override: str) -> BatchMetadata:
    print(">>> Loading batch metadata")

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
