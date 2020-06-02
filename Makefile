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

# table "dbo.WRKFLW_INSTNC"
TABLE_LOAD_PATH := "$(TABLE_BASE_STORAGE)/dbo.WRKFLW_INSTNC"
TABLE_CHANGES_PATH := "$(TABLE_BASE_STORAGE)/dbo.WRKFLW_INSTNC__ct"
TABLE_DELTA_PATH := "$(DELTA_BASE_STORAGE)/WRKFLW_INSTNC"

.PHONY: pload
pload:
	@rm -rf $(DELTA_BASE_STORAGE) && mkdir -p $(DELTA_BASE_STORAGE)
	@spark-submit ./transform/processing_load.py \
		-l $(TABLE_LOAD_PATH) \
		-d $(TABLE_DELTA_PATH)

.PHONY: pchanges
pchanges:
	@spark-submit ./transform/processing_changes.py
