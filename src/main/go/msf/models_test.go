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

func TestAllMeasures_AddMeasure(t *testing.T) {
	// Create a new instance of AllMeasures
	allMeasures := NewAggregator()

	// Add measures to the AllMeasures instance
	allMeasures.Add(measure{"Location1", 10 * FLOAT2INT})
	allMeasures.Add(measure{"Location2", 20 * FLOAT2INT})

	// Verify that the measures were added correctly
	require.Equal(t, 2, len(allMeasures.Locations))
	require.EqualValues(t, "10.0/10.0/10.0", allMeasures.Locations["Location1"].String())

	allMeasures.Add(measure{"Location1", 20 * FLOAT2INT})
	require.EqualValues(t, "10.0/15.0/20.0", allMeasures.Locations["Location1"].String())
}

func BenchmarkFullRun(b *testing.B) {
	const inputFilePath = "measurements-bench.txt"
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0666)
	require.NoError(b, err)
	for i := 0; i < b.N; i++ {
		input, err := os.Open(inputFilePath)
		require.NoError(b, err)

		allMeasures := NewAggregator().Run(inputFilePath, 0, 0)
		allMeasures.Print(devNull)

		input.Close()
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
			// Read the input file
			inputFilePath := filepath.Join(samplesDir, file.Name())

			allMeasures := NewAggregator().Run(inputFilePath, 0, 0)

			// Call the Print() method
			var buf bytes.Buffer
			allMeasures.Print(&buf)

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
