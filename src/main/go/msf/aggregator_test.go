package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// SETUP
// Importantly you need to call Run() once you've done what you need
func TestMain(m *testing.M) {
	log.SetOutput(ioutil.Discard)
	os.Exit(m.Run())
}

func TestAllMeasures_AddMeasure(t *testing.T) {
	// Create a new instance of AllMeasures
	allMeasures := NewAggregator()

	// Add measures to the AllMeasures instance
	allMeasures.Add("Location1;10.0")
	allMeasures.Add("Location2;20.0")

	// Verify that the measures were added correctly
	require.Equal(t, 2, len(allMeasures.data))
	require.EqualValues(t, "10.0/10.0/10.0", allMeasures.data["Location1"].String())

	allMeasures.Add("Location1;20.0")
	require.EqualValues(t, "10.0/15.0/20.0", allMeasures.data["Location1"].String())
}

func BenchmarkProcessFile(b *testing.B) {
	const samplesDir = "../../../test/resources/samples/"
	const inputFilePath = samplesDir + "measurements.bench"
	for i := 0; i < b.N; i++ {
		NewAggregator().Run(inputFilePath, io.Discard)
	}
}

func TestAllMeasures_Print(t *testing.T) {
	// Create a new instance of AllMeasures

	// Define the directory where the sample files are located
	const samplesDir = "../../../test/resources/samples/"

	// Iterate over the sample files
	files, err := os.ReadDir(samplesDir)
	if err != nil {
		t.Fatalf("Failed to read sample files: %v", err)
	}

	for _, file := range files {
		// Skip directories
		if file.IsDir() {
			continue
		}
		// Check if the file is an input file
		if filepath.Ext(file.Name()) != ".txt" {
			continue
		}
		ok := t.Run(file.Name(), func(t *testing.T) {
			// Run
			inputFilePath := filepath.Join(samplesDir, file.Name())
			var buf bytes.Buffer
			NewAggregator().Run(inputFilePath, &buf)

			// Define the expected output file path
			baseName := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
			expectedOutputFilePath := filepath.Join(samplesDir, fmt.Sprintf("%s.out", baseName))

			// Read the expected output file
			expectedOutput, err := os.ReadFile(expectedOutputFilePath)
			require.NoError(t, err)
			require.EqualValues(t, string(expectedOutput), buf.String())
		})
		require.True(t, ok, "failed on: ", file.Name())
	}
}
