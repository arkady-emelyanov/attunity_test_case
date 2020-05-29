package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	logFile := "./run.log"
	fp, err := os.OpenFile(logFile, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	headerLine := fmt.Sprintf("###### Run: %s\n", time.Now())
	fp.WriteString(headerLine)

	argsLine := fmt.Sprintf("> Args: %s\n", strings.Join(os.Args, " "))
	fp.WriteString(argsLine)

	envSlice := os.Environ()
	for _, s := range envSlice {
		envLine := fmt.Sprintf("> %s\n", s)
		fp.WriteString(envLine)
	}

	fp.WriteString("\n\n")
	fp.Sync()
	fp.Close()
}
