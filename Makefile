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

## Common (Delta and Hudi)
TABLE_BASE_STORAGE := "/Users/arkady/Projects/disney/spark_data"

## Delta Lake (delta.io)
DELTA_BASE_STORAGE := "$(TABLE_BASE_STORAGE)/out_delta"
DELTA_LIBRARY_JAR := "/Users/arkady/Projects/tools/libs/delta-core_2.11-0.6.1.jar"

DELTA_TABLE_NAME := "dbo.test_changing_load"
DELTA_LOAD_PATH := "$(TABLE_BASE_STORAGE)/$(DELTA_TABLE_NAME)"
DELTA_CHANGES_PATH := "$(TABLE_BASE_STORAGE)/$(DELTA_TABLE_NAME)__ct"
DELTA_PATH := "$(DELTA_BASE_STORAGE)/$(DELTA_TABLE_NAME)__delta"
DELTA_SDC_PATH := "$(DELTA_BASE_STORAGE)/$(DELTA_TABLE_NAME)__sdc"
DELTA_SNAPSHOT_PATH := "$(DELTA_BASE_STORAGE)/$(DELTA_TABLE_NAME)__snapshot"

.PHONY: delta_clean
delta_clean:
	@echo "### Clearing up $(DELTA_BASE_STORAGE)"
	@rm -rf $(DELTA_BASE_STORAGE) && mkdir -p $(DELTA_BASE_STORAGE)

.PHONY: delta_load
delta_load: delta_clean
	@echo "### Performing initial delta table load..."
	@spark-submit \
		--master local[*] \
		./transform/delta_load.py \
			--delta-library-jar $(DELTA_LIBRARY_JAR) \
			--delta-path $(DELTA_PATH) \
			--load-path $(DELTA_LOAD_PATH) \
			--changes-path $(DELTA_CHANGES_PATH) \
			--snapshot-path $(DELTA_SNAPSHOT_PATH)

.PHONY: delta_changes
delta_changes:
	@echo "### Processing incremental delta table changes..."
	@spark-submit \
		--master local[*] \
		./transform/delta_changes.py \
			--delta-library-jar $(DELTA_LIBRARY_JAR) \
			--delta-path $(DELTA_PATH) \
			--load-path $(DELTA_LOAD_PATH) \
			--changes-path $(DELTA_CHANGES_PATH) \
			--snapshot-path $(DELTA_SNAPSHOT_PATH)

.PHONY: delta_changes_scd
delta_changes_scd:
	@echo "### Processing SCD delta table changes..."
	@spark-submit \
		--master local[*] \
		./transform/delta_changes_scd.py \
			--delta-library-jar $(DELTA_LIBRARY_JAR) \
			--delta-path $(DELTA_PATH) \
			--delta-scd-path $(DELTA_SDC_PATH) \
			--load-path $(DELTA_LOAD_PATH) \
			--changes-path $(DELTA_CHANGES_PATH) \
			--snapshot-path $(DELTA_SNAPSHOT_PATH)

.PHONY: delta_snapshot
delta_snapshot:
	@echo "### Performing delta table snapshot..."
	@spark-submit \
		--master local[*] \
		./transform/delta_snapshot.py \
			--delta-library-jar $(DELTA_LIBRARY_JAR) \
			--delta-path $(DELTA_PATH) \
			--load-path $(DELTA_LOAD_PATH) \
			--changes-path $(DELTA_CHANGES_PATH) \
			--snapshot-path $(DELTA_SNAPSHOT_PATH)

.PHONY: delta_vacuum
delta_vacuum:
	@echo "### Performing vacuum..."
	@spark-submit \
		--master local[*] \
		./transform/delta_vacuum.py \
		--delta-library-jar $(DELTA_LIBRARY_JAR) \
		--delta-path $(DELTA_PATH) \
		--load-path $(DELTA_LOAD_PATH) \
		--changes-path $(DELTA_CHANGES_PATH) \
		--snapshot-path $(DELTA_SNAPSHOT_PATH)

## Apache Hudi

HUDI_BASE_STORAGE := "$(TABLE_BASE_STORAGE)/out_hudi"

#HUDI_TABLE_NAME := "dbo.TEST"
HUDI_TABLE_NAME := "dbo.test_changing_load"
HUDI_LOAD_PATH := "$(TABLE_BASE_STORAGE)/$(HUDI_TABLE_NAME)"
HUDI_CHANGES_PATH := "$(TABLE_BASE_STORAGE)/$(HUDI_TABLE_NAME)__ct"
HUDI_PATH := "$(HUDI_BASE_STORAGE)/$(HUDI_TABLE_NAME)__hudi"
HUDI_SNAPSHOT_PATH := "$(HUDI_BASE_STORAGE)/$(HUDI_TABLE_NAME)__snapshot"

.PHONY: hudi_clean
hudi_clean:
	@echo "### Clearing up $(HUDI_BASE_STORAGE)"
	@rm -rf $(HUDI_BASE_STORAGE) && mkdir -p $(HUDI_BASE_STORAGE)

.PHONY: hudi_load
hudi_load: hudi_clean
	@echo "### Performing initial load..."
	@spark-submit \
		--master local[*] \
		./transform/hudi_load.py \
		--load-path $(HUDI_LOAD_PATH) \
		--table-name $(HUDI_TABLE_NAME) \
		--hudi-path $(HUDI_PATH) \
		--snapshot-path $(HUDI_SNAPSHOT_PATH)

.PHONY: hudi_changes
hudi_changes:
	@echo "### Processing incremental changes..."
	@spark-submit \
		--master local[*] \
		./transform/hudi_changes.py \
		--load-path $(HUDI_CHANGES_PATH) \
		--table-name $(HUDI_TABLE_NAME) \
		--hudi-path $(HUDI_PATH) \
		--snapshot-path $(HUDI_SNAPSHOT_PATH)

.PHONY: hudi_changes_scd
hudi_changes_scd:
	@echo "### Processing SCD Type changes..."
	@spark-submit \
		--master local[*] \
		./transform/hudi_changes_scd.py \
		--load-path $(HUDI_CHANGES_PATH) \
		--table-name $(HUDI_TABLE_NAME) \
		--hudi-path $(HUDI_PATH) \
		--snapshot-path $(HUDI_SNAPSHOT_PATH)

.PHONY: hudi_snapshot
hudi_snapshot:
	@echo "### Exporting snapshot..."
	@spark-submit \
		--master local[*] \
		./transform/hudi_snapshot.py \
		--load-path $(HUDI_CHANGES_PATH) \
		--table-name $(HUDI_TABLE_NAME) \
		--hudi-path $(HUDI_PATH) \
		--snapshot-path $(HUDI_SNAPSHOT_PATH)
