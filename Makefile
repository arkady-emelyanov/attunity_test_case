FILE = "$(shell find /Users/arkady/Projects/disney/spark_data/out/dbo.test_changing_load/ -name '*.parquet' | head -n 1)"

.PHONY: mssql
mssql:
	docker exec -it mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P '1234abc7643Z'

.PHONY: build
build:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./artifacts/producer.exe ./producer/main.go
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./artifacts/postprocess.exe ./post-processing/main.go


.PHONY: parquet
parquet:
	rm -rf /Users/arkady/Projects/disney/spark_data/out
	mkdir -p /Users/arkady/Projects/disney/spark_data/out
	python3 ./transform/parquet.py

.PHONY: pinfo
pinfo:
	parquet-tools schema $(FILE)
	parquet-tools cat -j $(FILE)
