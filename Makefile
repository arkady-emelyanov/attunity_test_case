## attunity story
.PHONY: all
all:
	@echo "Nothing to-do"

.PHONY: mssql
mssql:
	@docker exec -it mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P '1234abc7643Z'

.PHONY: build
build:
	@CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./artifacts/producer.exe ./producer/main.go
	@CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./artifacts/postprocess.exe ./post-processing/main.go

## spark story
TABLE_BASE_STORAGE := "/Users/arkady/Projects/disney/spark_data"
DELTA_BASE_STORAGE := "/Users/arkady/Projects/disney/spark_data/out"
DELTA_LIBRARY_JAR := "/Users/arkady/Projects/tools/libs/delta-core_2.11-0.6.1.jar"

## working on single table
TABLE_NAME := "dbo.WRKFLW_INSTNC"
#TABLE_NAME := "dbo.test_schema_change"
#TABLE_NAME := "dbo.test_changing_load"
#TABLE_NAME := "dbo.test_bulk_load"
#TABLE_NAME := "dbo.WRKFLW_EVNT"
TABLE_LOAD_PATH := "$(TABLE_BASE_STORAGE)/$(TABLE_NAME)"
TABLE_CHANGES_PATH := "$(TABLE_BASE_STORAGE)/$(TABLE_NAME)__ct"
TABLE_DELTA_PATH := "$(DELTA_BASE_STORAGE)/$(TABLE_NAME)__delta"
TABLE_SNAPSHOT_PATH := "$(DELTA_BASE_STORAGE)/$(TABLE_NAME)__snapshot"

.PHONY: clean
clean:
	@echo "### Clearing up $(DELTA_BASE_STORAGE)"
	@rm -rf $(DELTA_BASE_STORAGE) && mkdir -p $(DELTA_BASE_STORAGE)

.PHONY: package
package:
	@echo "### Packing transformation code"
	@find . -type d -name '__pycache__' | xargs rm -rf
	@zip -r transform.zip transform/*

.PHONY: load
load: clean package
	@echo "### Performing initial delta table load..."
	@spark-submit \
		--py-files transform.zip \
		./job_runner.py \
			--task initial_load_task \
			--delta-library-jar $(DELTA_LIBRARY_JAR) \
			--delta-path $(TABLE_DELTA_PATH) \
			--load-path $(TABLE_LOAD_PATH) \
			--changes-path $(TABLE_CHANGES_PATH) \
			--snapshot-path $(TABLE_SNAPSHOT_PATH)

.PHONY: changes
changes: package
	@echo "### Processing incremental delta table changes..."
	@spark-submit \
		--py-files transform.zip \
		./job_runner.py \
			--task apply_changes_task \
			--delta-library-jar $(DELTA_LIBRARY_JAR) \
			--delta-path $(TABLE_DELTA_PATH) \
			--load-path $(TABLE_LOAD_PATH) \
			--changes-path $(TABLE_CHANGES_PATH) \
			--snapshot-path $(TABLE_SNAPSHOT_PATH)

.PHONY: snapshot
snapshot: package
	@echo "### Performing delta table snapshot..."
	@spark-submit \
		--py-files transform.zip \
		./job_runner.py \
			--task snapshot_task \
			--delta-library-jar $(DELTA_LIBRARY_JAR) \
			--delta-path $(TABLE_DELTA_PATH) \
			--load-path $(TABLE_LOAD_PATH) \
			--changes-path $(TABLE_CHANGES_PATH) \
			--snapshot-path $(TABLE_SNAPSHOT_PATH)

.PHONY: vacuum
vacuum: package
	@echo "### Performing vacuum..."
	@spark-submit \
		--py-files transform.zip \
		./job_runner.py \
			--task vacuum \
			--delta-library-jar $(DELTA_LIBRARY_JAR) \
			--delta-path $(TABLE_DELTA_PATH) \
			--load-path $(TABLE_LOAD_PATH) \
			--changes-path $(TABLE_CHANGES_PATH) \
			--snapshot-path $(TABLE_SNAPSHOT_PATH)
