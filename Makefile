## attunity story
.PHONY: mssql
mssql:
	docker exec -it mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P '1234abc7643Z'

.PHONY: build
build:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./artifacts/producer.exe ./producer/main.go
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./artifacts/postprocess.exe ./post-processing/main.go

## spark story
TABLE_BASE_STORAGE := "/Users/arkady/Projects/disney/spark_data"
DELTA_BASE_STORAGE := "/Users/arkady/Projects/disney/spark_data/out"
DELTA_LIBRARY_JAR := "/Users/arkady/Projects/tools/libs/delta-core_2.11-0.6.1.jar"

## working on single table
TABLE_LOAD_PATH := "$(TABLE_BASE_STORAGE)/dbo.WRKFLW_INSTNC"
TABLE_CHANGES_PATH := "$(TABLE_BASE_STORAGE)/dbo.WRKFLW_INSTNC__ct"
TABLE_DELTA_PATH := "$(DELTA_BASE_STORAGE)/WRKFLW_INSTNC__delta"
TABLE_SNAPSHOT_PATH := "$(DELTA_BASE_STORAGE)/WRKFLW_INSTNC__snapshot"

.PHONY: load
load:
	@echo "### Performing initial delta table creation..."
	@rm -rf $(DELTA_BASE_STORAGE) && mkdir -p $(DELTA_BASE_STORAGE)
	@spark-submit ./transform/load.py \
		--delta-library-jar $(DELTA_LIBRARY_JAR) \
		--delta-path $(TABLE_DELTA_PATH) \
		--load-path $(TABLE_LOAD_PATH) \
		--changes-path $(TABLE_CHANGES_PATH) \
		--snapshot-path $(TABLE_SNAPSHOT_PATH)

.PHONY: changes
changes:
	@echo "### Applying delta table changes..."
	@spark-submit ./transform/changes.py \
		--delta-library-jar $(DELTA_LIBRARY_JAR) \
		--delta-path $(TABLE_DELTA_PATH) \
		--load-path $(TABLE_LOAD_PATH) \
		--changes-path $(TABLE_CHANGES_PATH) \
		--snapshot-path $(TABLE_SNAPSHOT_PATH)

.PHONY: snapshot
snapshot:
	@echo "### Performing delta table point-in-time snapshot..."
	@spark-submit ./transform/snapshot.py \
		--delta-library-jar $(DELTA_LIBRARY_JAR) \
		--delta-path $(TABLE_DELTA_PATH) \
		--load-path $(TABLE_LOAD_PATH) \
		--changes-path $(TABLE_CHANGES_PATH) \
		--snapshot-path $(TABLE_SNAPSHOT_PATH)
