package main

import (
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

type ParallelAggregator struct {
	filename    string
	fileSize    int64
	chunkSize   int64
	chunks      int
	results     chan *AllMeasures
	finalResult *AllMeasures
}

func ProcessFile(filename string, w io.Writer, chunks int) {
	if chunks < 1 {
		chunks = runtime.NumCPU()
	}

	aggregator := NewParallelAggregator(filename, chunks)
	aggregator.Run()
	aggregator.Print(w)
}

func NewParallelAggregator(filename string, chunks int) *ParallelAggregator {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		log.Fatal(err)
	}

	return &ParallelAggregator{
		filename:    filename,
		fileSize:    fileInfo.Size(),
		chunks:      chunks,
		chunkSize:   fileInfo.Size() / int64(chunks),
		results:     make(chan *AllMeasures, chunks),
		finalResult: NewAggregator(),
	}
}

func (pa *ParallelAggregator) Run() {
	var wg sync.WaitGroup
	wg.Add(pa.chunks)

	for i := 0; i < pa.chunks; i++ {
		go pa.ProcessChunk(i, &wg)
	}

	go func() {
		wg.Wait()
		close(pa.results)
	}()

	for res := range pa.results {
		pa.finalResult.Merge(res)
	}
}

func (pa *ParallelAggregator) ProcessChunk(chunkIndex int, wg *sync.WaitGroup) {
	defer wg.Done()

	start := int64(chunkIndex) * pa.chunkSize
	end := int64(chunkIndex+1) * pa.chunkSize

	if chunkIndex == pa.chunks-1 {
		end = pa.fileSize
	}
	aggregator := NewAggregator().Run(pa.filename, start, end)
	pa.results <- aggregator
}

func (pa *ParallelAggregator) Print(w io.Writer) {
	pa.finalResult.Print(w)
}
