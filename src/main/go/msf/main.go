package main

import (
	"os"
)

func main() {
	fName := os.Args[1]
	ProcessFile(fName, os.Stdout, 0)
}
