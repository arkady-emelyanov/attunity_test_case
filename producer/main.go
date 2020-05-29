package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	hostname = flag.String("hostname", "localhost", "MSSQL hostname")
	database = flag.String("database", "test", "MSSQL database")
	username = flag.String("username", "sa", "MSSQL User")

	// NOT: default password is for MS SQL testing container and NOT for real system.
	password = flag.String("password", "1234abc7643Z", "MSSQL Password")

	port = flag.Int("port", 1433, "MSSQL Port")
	help = flag.Bool("help", false, "Display help")
	mode = flag.String("mode", "inf", "Run mode")
)

const (
	bulkMaxArgs    = 1000  // number of arguments for MS SQL batch
	bulkCounterMax = 10000 // number of batches to perform for each worker
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

func infiniteProducerWorker(workerId int, startId int) {
	d, err := getDatabase()
	if err != nil {
		panic(err)
	}

	// create table test_bulk_load (id int, name varchar(50))
	// start loading data
	var bulkArgs []interface{}
	var bulkVals []string

	bulkPosArgsCounter := 0
	bulkCurrent := 0
	bulkCounterVal := 0

	fmt.Printf("[%d] Worker started...\n", workerId)
	tt := time.Now()
	for {
		if bulkCurrent == bulkMaxArgs {
			// prepare statement
			sqlInsert := fmt.Sprintf(`INSERT INTO test_bulk_load (ID, NAME) VALUES %s`,
				strings.Join(bulkVals, ","),
			)
			stmt, err := d.Prepare(sqlInsert)
			if err != nil {
				panic(err)
			}

			// execute it
			if _, err := stmt.Exec(bulkArgs...); err != nil {
				panic(err)
			}

			// report only each 100th batch
			if (bulkCounterVal > 0) && (bulkCounterVal%100 == 0) {
				took := time.Now().Sub(tt)
				tt = time.Now()
				fmt.Printf("[%d] Bulk=%d, took=%s\n", workerId, bulkCounterVal, took)
			}

			// close statement
			if err := stmt.Close(); err != nil {
				panic(err)
			}

			// reset memory
			bulkCurrent = 0
			bulkArgs = nil
			bulkVals = nil
			bulkPosArgsCounter = 0

			// increase current bulk load
			bulkCounterVal++
		}

		// stop after 100 mil
		if bulkCounterVal >= bulkCounterMax {
			break
		}

		name := randomString(49)
		bulkArgs = append(bulkArgs, startId)
		bulkArgs = append(bulkArgs, name)

		argPos := fmt.Sprintf("(@p%d, @p%d)", bulkPosArgsCounter+1, bulkPosArgsCounter+2)
		bulkVals = append(bulkVals, argPos)

		bulkCurrent++
		bulkPosArgsCounter += 2
		startId++
	}
}

func infiniteProducer() {
	var wg sync.WaitGroup

	tt := time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			nextStartId := i*bulkMaxArgs*bulkCounterMax + 1
			infiniteProducerWorker(i, nextStartId)
			wg.Done()
		}(i)
	}

	fmt.Println("Load started, hit Ctrl+C to terminate...")
	wg.Wait()
	fmt.Printf("Load done, took=%s\n", time.Now().Sub(tt))
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
