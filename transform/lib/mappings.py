from pyspark.sql.types import *

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
