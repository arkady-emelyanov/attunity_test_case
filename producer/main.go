package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	hostname = flag.String("hostname", "localhost", "MSSQL hostname")
	database = flag.String("database", "test", "MSSQL database")
	username = flag.String("username", "sa", "MSSQL User")
	password = flag.String("password", "1234abc7643Z", "MSSQL Password")
	port     = flag.Int("port", 1433, "MSSQL Port")
	help     = flag.Bool("help", false, "Display help")
	mode     = flag.String("mode", "inf", "Run mode")
)

func randomString(n int) string {
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		j := rand.Intn(len(letters) - 1)
		b[i] = letters[j]
	}
	return string(b)
}

func getDatabase() (*sql.DB, error) {
	query := url.Values{}
	query.Add("database", *database)

	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(*username, *password),
		Host:   fmt.Sprintf("%s:%d", *hostname, *port),
		// Path:  instance, // if connecting to an instance instead of a port
		RawQuery: query.Encode(),
	}

	return sql.Open("sqlserver", u.String())
}

func infiniteProducer() {
	d, err := getDatabase()
	if err != nil {
		panic(err)
	}

	// create table test_bulk_load (id int, name varchar(50))
	// start loading data
	var bulkArgs []interface{}
	var bulkVals []string

	startId := 0
	bulkMaxArgs := 1000 // max: 1000
	bulkCurrent := 0
	bulkCounterVal := 0
	bulkCounterMax := 100000

	fmt.Printf("Producing messages, startId=%d, hit Ctrl+C to stop...\n", startId)
	tt := time.Now()
	for {
		if bulkCurrent == bulkMaxArgs {
			// prepare statement
			stmt, err := d.Prepare(
				fmt.Sprintf(`INSERT INTO test_bulk_load (ID, NAME) VALUES %s`,
					strings.Join(bulkVals, ","),
				),
			)
			if err != nil {
				panic(err)
			}
			// execute it
			if _, err := stmt.Exec(bulkArgs...); err != nil {
				panic(err)
			}

			took := time.Now().Sub(tt)
			tt = time.Now()
			fmt.Printf("Bulk=%d, took=%s\n", bulkCounterVal, took)

			bulkCurrent = 0
			bulkArgs = nil
			bulkVals = nil
			bulkCounterVal++
		}

		// stop after 100 mil
		if bulkCounterVal >= bulkCounterMax {
			break
		}

		name := randomString(49)
		bulkArgs = append(bulkArgs, startId)
		bulkArgs = append(bulkArgs, name)
		bulkVals = append(bulkVals, "(@p1, @p2)")

		bulkCurrent++
		startId++
	}
}

func main() {
	flag.Parse()
	if *help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	// mode
	switch *mode {
	case "inf":
		infiniteProducer()

	default:
		fmt.Printf("Unknown mode: %s\n", *mode)
		os.Exit(1)
	}
}
