# Qlik changes/data files processing
#
# Spark data: /Users/arkady/Projects/disney/spark_data
# Data files: /Users/arkady/Projects/disney/spark_data/dbo.test_changing_load
# Changes files: /Users/arkady/Projects/disney/spark_data/dbo.test_changing_load__ct

import json
import os.path

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType

DATA_PATH = "/Users/arkady/Projects/disney/spark_data/dbo.test_changing_load"
CHANGES_PATH = "/Users/arkady/Projects/disney/spark_data/dbo.test_changing_load__ct"
OUTPUT_PATH = "/Users/arkady/Projects/disney/spark_data/out/dbo.test_changing_load"

# Attunity SQL Server schema mappings
TYPE_MAPPINGS = {
    "INT4": IntegerType,
    "STRING": StringType,
}

spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# load all .dfm files from data_path and validate all of them
# has same structure

# 1. list files
print(">>> Listing DFM files")
dfmFiles = []
datFiles = []
for s in os.listdir(DATA_PATH):
    if s.endswith(".dfm"):
        dfmFiles.append(os.path.join(DATA_PATH, s))
    if s.endswith(".json.gz"):
        datFiles.append(os.path.join(DATA_PATH, s))

# 2. sort
print(">>> Sorting DFM files")

# 3. validate schema
print(">>> Validating DFM schema")
rddColumns = None
for dfmFilePath in dfmFiles:
    with open(dfmFilePath) as schemaFile:
        obj = json.loads(schemaFile.read())
        if type(obj) != dict:
            raise Exception(f"Not a JSON in: {dfmFilePath}")

        if obj['dfmVersion'] != "1.1":
            raise Exception("Unknown dfmVersion")

        if obj['formatInfo']["format"] != "json":
            raise Exception(f"Unknown format in {dfmFilePath}")

        columns = obj['dataInfo']['columns']
        if rddColumns is None:
            rddColumns = columns
            continue

        # validate current columns and firstColumns
        if columns != rddColumns:
            raise Exception(f"Different number of columns in {dfmFilePath}")

if rddColumns is None:
    raise Exception("No columns found")

# 4. load rdd files
print(">>> Processing RDDs")
schema = StructType()
for col in rddColumns:
    col_type = col['type']
    schema.add(col['name'], TYPE_MAPPINGS[col_type]())

files = spark.sparkContext.textFile(",".join(datFiles))
df = spark.read.json(files, schema=schema)

df.printSchema()
df.show(50)
df.write.parquet(OUTPUT_PATH)
