package main

import (
	"io"
	"log"
	"log/slog"
	"os"
	"runtime"
	"sync"
)

func ProcessFile(filename string, w io.Writer, chunks int) {
	if chunks < 1 {
		chunks = runtime.NumCPU()
	}

	aggregator := NewParallelAggregator(chunks)
	defer aggregator.Done()
	aggregator.Process(filename, w)
}

type ParallelAggregator struct {
	workers int
	wg      sync.WaitGroup
	tasks   chan task
}

type task struct {
	filename string
	start    int64
	end      int64
	resp     chan *MeasurementAggregator
}

func NewParallelAggregator(workers int) *ParallelAggregator {
	pa := &ParallelAggregator{
		workers: workers,
		tasks:   make(chan task, workers),
	}
	for i := 0; i < workers; i++ {
		pa.wg.Add(1)
		go func() {
			defer pa.wg.Done()
			for task := range pa.tasks {
				agg := NewAggregator().processChunk(task.filename, task.start, task.end)
				task.resp <- agg
			}
		}()
	}
	return pa
}

func (pa *ParallelAggregator) Done() {
	close(pa.tasks)
	pa.wg.Wait()
}

func (pa *ParallelAggregator) Process(filename string, w io.Writer) *ParallelAggregator {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		log.Fatal(err)
	}

	chunks := pa.workers
	fileSize := fileInfo.Size()
	chunkSize := fileSize / int64(chunks)

	slog.Info("Processing file..",
		"filename", filename,
		"chunks", chunks,
		"chunkSizeMiB", chunkSize/(1024*1024),
		"fileSizeMiB", fileSize/(1024*1024),
	)
	resultsChan := make(chan *MeasurementAggregator, chunks)
	defer close(resultsChan)

	for i := 0; i < chunks; i++ {
		start := int64(i) * chunkSize
		end := int64(i+1) * chunkSize
		if i == chunks-1 {
			end = 0
		}
		pa.tasks <- task{filename, start, end, resultsChan}
	}

	result := NewAggregator()
	for i := 0; i < chunks; i++ {
		res := <-resultsChan
		result.Merge(res)
	}

	result.writeTo(w)
	return pa
}
