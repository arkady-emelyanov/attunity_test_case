# Qlik Replicate (Attunity) CDC: S3

Number of test cases for Qlick Replicate.
* Change Data Capture -> S3 target
* CDC transform (table level: parquet)
* CDC transform (join level: parquet)

## CDC

* Apache Spark 2.4.6
* Delta Lake 0.6.1
* Apache Hudi 0.5.3

### Initial Load

> Check out how initial load batch will look like in S3

* Run complete table data load
* Setup replication to S3
* How would initial batch on S3 will look like?

### Incremental load (INSERT)

> Check out how incremental batch with INSERT DML
will look like in S3

* For configured table start loading data (incremental update)
* How would batch on S3 will look like?
* Play with options for S3 target

### Incremental load (UPDATE/DELETE)

> Check out how incremental batch with UPDATE/DELETE DML
will look like in S3

### Post-processing script

> Check out which arguments Attunity pass to 
Post-processing script when data is dumped to S3

* Add post-processing script to S3 target
* Run small incremental batch load
* Check post-processing script log
* Disable post-processing script

### Schema change

> Check out how Attunity will react on schema change DDL
and how data will be represented on S3

* Run small incremental batch load
* Run schema change (simulate schema migration)
* Run small incremental batch load
* Check S3 target

## Transformations

> Check out for possible options to transform JSON and Metadata
into Parquet file

### Physical layer

### Join layer

