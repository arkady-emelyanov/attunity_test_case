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
	_, _ = fp.WriteString(headerLine)

	argsLine := fmt.Sprintf("> Args: %s\n", strings.Join(os.Args, " "))
	_, _ = fp.WriteString(argsLine)

	envSlice := os.Environ()
	for _, s := range envSlice {
		envLine := fmt.Sprintf("> %s\n", s)
		_, _ = fp.WriteString(envLine)
	}

	_, _ = fp.WriteString("\n\n")
	_ = fp.Sync()
	_ = fp.Close()
}
