# MS SQLServer bulk load script

> default password is for MS SQL testing container and NOT for real system.

Target table DDL:
```
create table test_bulk_load (
    id int, 
    name varchar(50)
)
```

Command line options:
```
  -database string
    	MSSQL database (default "test")
  -help
    	Display help
  -hostname string
    	MSSQL hostname (default "localhost")
  -password string
    	MSSQL Password (default "1234abc7643Z")
  -port int
    	MSSQL Port (default 1433)
  -username string
    	MSSQL User (default "sa")
```
