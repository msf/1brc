package main

import (
	"os"
	"strconv"
)

func main() {
	fName := os.Args[1]
	var chunks int
	if len(os.Args) > 2 {
		chunks, _ = strconv.Atoi(os.Args[2])
	}
	ProcessFile(fName, os.Stdout, chunks)
}
