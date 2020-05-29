package main

import (
	"fmt"
	"os"
	"time"
)

func main() {
	logFile := "./run.log"
	fp, err := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	headerLine := fmt.Sprintf("###### Run: %s\n", time.Now())
	_, _ = fp.WriteString(headerLine)

	// TODO: list the S3 bucket key prefix and log results
	// TODO: list reference directory

	_, _ = fp.WriteString("\n\n")
	_ = fp.Sync()
	_ = fp.Close()
}
