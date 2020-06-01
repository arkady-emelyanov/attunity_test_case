.PHONY: mssql
mssql:
	docker exec -it mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P '1234abc7643Z'

.PHONY: build
build:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./artifacts/producer.exe ./producer/main.go
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./artifacts/postprocess.exe ./post-processing/main.go

## spark story

.PHONY: pload
pload:
	rm -rf /Users/arkady/Projects/disney/spark_data/out
	mkdir -p /Users/arkady/Projects/disney/spark_data/out
	python3 ./transform/processing_load.py

.PHONY: pchanges
pchanges:
	rm -rf /Users/arkady/Projects/disney/spark_data/out
	mkdir -p /Users/arkady/Projects/disney/spark_data/out
	python3 ./transform/processing_changes.py
