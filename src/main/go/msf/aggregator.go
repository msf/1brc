package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

type MeasurementAggregator struct {
	data map[string]*aggregate
}

func NewAggregator() *MeasurementAggregator {
	return &MeasurementAggregator{
		data: make(map[string]*aggregate),
	}
}

func (a *MeasurementAggregator) process(filename string, w io.Writer) {
	a.processChunk(filename, 0, 0).writeTo(w)
}

func (a *MeasurementAggregator) processChunk(filename string, start, end int64) *MeasurementAggregator {
	file, err := os.Open(filename)
	assertNoErr(err)
	defer file.Close()
	curr, err := file.Seek(start, 0)
	assertNoErr(err)
	scanner := bufio.NewScanner(file)
	first := true
	for scanner.Scan() && (end == 0 || curr <= end) {
		curr += int64(len(scanner.Bytes())) + 1
		if first && start != 0 {
			// skip the first line if we're starting in non zero offset, incomplete
			first = false
			continue
		}
		a.Add(scanner.Text())
	}

	err = scanner.Err()
	assertNoErr(err)
	return a
}

func (a *MeasurementAggregator) Add(line string) {
	loc, temp := parse(line)
	if rec, ok := a.data[loc]; !ok {
		a.data[loc] = &aggregate{Max: temp, Min: temp, Sum: temp, Count: 1}
	} else {
		rec.Add(temp)
	}
}

func (a *MeasurementAggregator) Merge(b *MeasurementAggregator) {
	for location, aggregate := range b.data {
		if rec, ok := a.data[location]; !ok {
			a.data[location] = aggregate
		} else {
			rec.Merge(aggregate)
		}
	}
}

func (a *MeasurementAggregator) writeTo(dst io.Writer) {
	sortedLocations := make([]string, 0, len(a.data))
	for location := range a.data {
		sortedLocations = append(sortedLocations, location)
	}
	sort.Strings(sortedLocations)
	w := bufio.NewWriter(dst)
	w.WriteByte('{')
	for i, location := range sortedLocations {
		aggregate := a.data[location]
		if i > 0 {
			w.WriteString(", ")
		}
		w.WriteString(location)
		w.WriteByte('=')
		aggregate.writeTo(w)
	}
	w.WriteString("}\n")
	w.Flush()
}

type aggregate struct {
	Max   Temperature
	Min   Temperature
	Sum   Temperature
	Count uint32
}

func (a *aggregate) Add(temp Temperature) {
	a.Max = max(a.Max, temp)
	a.Min = min(a.Min, temp)
	a.Sum += temp
	a.Count++
}

func (a *aggregate) Merge(b *aggregate) {
	a.Max = max(a.Max, b.Max)
	a.Min = min(a.Min, b.Min)
	a.Sum += b.Sum
	a.Count += b.Count
}

func (a *aggregate) writeTo(w Writer) {
	w.WriteString(strconv.FormatFloat(float64(a.Min)/FLOAT2INT, 'f', 1, 64))
	w.WriteByte('/')
	w.WriteString(strconv.FormatFloat(a.Avg(), 'f', 1, 64))
	w.WriteByte('/')
	w.WriteString(strconv.FormatFloat(float64(a.Max)/FLOAT2INT, 'f', 1, 64))
}

func (a *aggregate) Avg() float64 {
	t := float64(a.Sum) / float64(a.Count*FLOAT2INT)
	return round(t)
}

func (a *aggregate) String() string {
	buf := bytes.Buffer{}
	a.writeTo(&buf)
	return buf.String()
}

type Temperature int32

const FLOAT2INT = 10

type Writer interface {
	io.StringWriter
	io.ByteWriter
}

func parse(s string) (string, Temperature) {
	loc, tmp, ok := strings.Cut(s, ";")
	if !ok {
		log.Fatal("parse error, line: ", s)
	}
	val, err := strconv.ParseFloat(tmp, 32)
	if err != nil {
		log.Fatalf("invalid temperature value: %v, line: %v, err: %v", tmp, s, err)
	}
	temp := Temperature(round(val) * FLOAT2INT)
	return loc, temp
}

// rounding floats to 1 decimal place with 0.05 rounding up to 0.1
func round(x float64) float64 {
	return math.Floor((x+0.05)*10) / 10
}
