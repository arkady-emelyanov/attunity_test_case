# MS SQLServer bulk load script

> default password is for MS SQL testing container and NOT for real system.

## Massive bulk load

Target table DDL:
```
create table test_bulk_load (
    id int, 
    name varchar(50)
)
```

## Changing bulk load

Target table DDL:
```
create table test_changing_load (
    id int, 
    name varchar(50)
)
```

Final result:
```
1> select * from test_changing_load;
2> go
id          name
----------- --------------------------------------------------
          1 number_one_changed
          6 number_six
          3 number_three
          4 number_four
          5 number_five_changed
          7 number_six

(6 rows affected)
```

## Misc

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
