package main

import (
	"bufio"
	"bytes"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
)

type Temperature int32
type hashSize uint32

type MeasurementAggregator struct {
	data      map[hashSize]*aggregate
	locations []string
	hasher    hash.Hash32
}

func NewAggregator() *MeasurementAggregator {
	return &MeasurementAggregator{
		data:      make(map[hashSize]*aggregate, 1000),
		locations: make([]string, 0, 1000),
		hasher:    fnv.New32a(),
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
		line := scanner.Bytes()
		curr += int64(len(line)) + 1
		if first && start != 0 {
			// skip the first line if we're starting in non zero offset, incomplete
			first = false
			continue
		}
		a.Add(line)
	}

	err = scanner.Err()
	assertNoErr(err)
	return a
}

func (a *MeasurementAggregator) Add(line []byte) {
	loc, temp := parse(line)
	id := a.hash(loc)
	if rec, ok := a.data[id]; !ok {
		sloc := string(loc)
		a.data[id] = &aggregate{Max: temp, Min: temp, Sum: temp, Count: 1, Location: sloc}
		a.locations = append(a.locations, sloc)
	} else {
		rec.Add(temp)
	}
}

func (a *MeasurementAggregator) hash(loc []byte) hashSize {
	a.hasher.Reset()
	a.hasher.Write(loc)
	return hashSize(a.hasher.Sum32())
}

func (a *MeasurementAggregator) Merge(b *MeasurementAggregator) {
	for location, aggregate := range b.data {
		if rec, ok := a.data[location]; !ok {
			a.data[location] = aggregate
			a.locations = append(a.locations, aggregate.Location)
		} else {
			rec.Merge(aggregate)
		}
	}
}

func (a *MeasurementAggregator) writeTo(dst io.Writer) {
	sort.Strings(a.locations)
	w := bufio.NewWriter(dst)
	w.WriteByte('{')
	for i, location := range a.locations {
		id := a.hash([]byte(location))
		aggregate := a.data[id]
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
	Max      Temperature
	Min      Temperature
	Sum      Temperature
	Count    uint32
	Location string
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

const FLOAT2INT = 10

type Writer interface {
	io.StringWriter
	io.ByteWriter
}

func parse(s []byte) ([]byte, Temperature) {
	idx := bytes.LastIndexByte(s, ';')
	if idx == -1 {
		log.Fatal("parse error, line: ", string(s))
	}
	loc := s[:idx]
	val := parseInt(s[idx+1:])
	temp := Temperature(val)
	return loc, temp
}

func parseInt(s []byte) int {
	num := 0
	sign := 1
	for i, r := range s {
		if i == 0 && r == '-' {
			sign = -1
			continue
		}
		if r < '0' || r > '9' {
			continue
		}
		num = num*10 + int(r-'0')
	}
	return num * sign
}

// rounding floats to 1 decimal place with 0.05 rounding up to 0.1
func round(x float64) float64 {
	return math.Floor((x+0.05)*10) / 10
}
