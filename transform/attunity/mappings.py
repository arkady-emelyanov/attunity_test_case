from pyspark.sql.types import DataType, IntegerType, StringType

# Attunity SQL Server schema mappings
TYPE_MAPPINGS = {
    "INT4": IntegerType,
    "STRING": StringType,
}


def get_type(source_type: str) -> DataType:
    return TYPE_MAPPINGS.get(source_type)()
