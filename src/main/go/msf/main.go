package main

import (
	"log"
	"os"
)

func main() {

	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	allReadings := NewAllMeasures()
	allReadings.ReadReadings(file)
	allReadings.Print(os.Stdout)
}
