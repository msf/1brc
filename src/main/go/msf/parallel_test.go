package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParallel_basic(t *testing.T) {
	const testfile = "../../../test/resources/samples/measurements-3.txt"
	const expectedOutputFilename = "../../../test/resources/samples/measurements-3.out"
	// Read the expected output file
	expectedOutput, err := os.ReadFile(expectedOutputFilename)
	require.NoError(t, err)

	aggregator := NewParallelAggregator(32)
	defer aggregator.Done()

	var buf bytes.Buffer
	aggregator.Process(testfile, &buf)

	// Verify the results
	require.EqualValues(t, string(expectedOutput), buf.String())
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
		aggregator := NewParallelAggregator(32)
		defer aggregator.Done()
		ok := t.Run(file.Name(), func(t *testing.T) {
			inputFilePath := filepath.Join(samplesDir, file.Name())
			var buf bytes.Buffer

			aggregator.Process(inputFilePath, &buf)

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

func BenchmarkParallelProcessFile(b *testing.B) {
	const samplesDir = "../../../test/resources/samples/"
	const inputFilePath = samplesDir + "measurements.bench"
	aggregator := NewParallelAggregator(16)
	defer aggregator.Done()

	for i := 0; i < b.N; i++ {
		aggregator.Process(inputFilePath, io.Discard)
	}
}
