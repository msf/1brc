package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParallel_basic(t *testing.T) {
	// Create a new instance of ParallelAggregator
	aggregator := NewParallelAggregator("../../../test/resources/samples/measurements-3.txt", 8)

	// Run the aggregator
	aggregator.Run()

	// Verify the results
	require.Equal(t, 2, len(aggregator.finalResult.Locations))
	require.Equal(t, "-15.0/1.3/20.0", aggregator.finalResult.Locations["Bosaso"].String())
}

func TestParallel_Samples(t *testing.T) {
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
			inputFilePath := filepath.Join(samplesDir, file.Name())
			var buf bytes.Buffer

			ProcessFile(inputFilePath, &buf, 32 /*lots of chunks */)

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

func BenchmarkParallelFullRun(b *testing.B) {
	const inputFilePath = "measurements-bench.txt"
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0666)
	require.NoError(b, err)
	for i := 0; i < b.N; i++ {
		ProcessFile(inputFilePath, devNull, 0)
	}
}
