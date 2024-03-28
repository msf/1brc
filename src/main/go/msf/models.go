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

type AllMeasures struct {
	Locations map[string]*aggregate
}

func NewAggregator() *AllMeasures {
	return &AllMeasures{Locations: make(map[string]*aggregate)}
}

func (a *AllMeasures) Run(fileName string, start, end int64) *AllMeasures {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	curr, err := file.Seek(start, 0)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(file)
	if end == 0 {
		end = math.MaxInt64
	}
	first := true
	for scanner.Scan() && curr <= end {
		curr += int64(len(scanner.Bytes())) + 1
		if first && start != 0 {
			first = false
			// skip the first line if we're starting in non zero offset, incomplete
			continue
		}
		var m measure
		s := scanner.Text()
		m.Parse(s)
		a.Add(m)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return a
}

func (a *AllMeasures) Add(m measure) {
	if rec, ok := a.Locations[m.Location]; !ok {
		a.Locations[m.Location] = &aggregate{Max: m.Temp, Min: m.Temp, Sum: m.Temp, Count: 1}
	} else {
		rec.Add(m)
	}
}

func (a *AllMeasures) Merge(b *AllMeasures) {
	for location, aggregate := range b.Locations {
		if rec, ok := a.Locations[location]; !ok {
			a.Locations[location] = aggregate
		} else {
			rec.Merge(aggregate)
		}
	}
}

func (a *AllMeasures) Print(dst io.Writer) {
	sortedLocations := make([]string, 0, len(a.Locations))
	for location := range a.Locations {
		sortedLocations = append(sortedLocations, location)
	}
	sort.Strings(sortedLocations)

	w := bufio.NewWriter(dst)

	w.WriteByte('{')
	for i, location := range sortedLocations {
		aggregate := a.Locations[location]
		if i > 0 {
			w.WriteByte(',')
			w.WriteByte(' ')
		}
		w.WriteString(location)
		w.WriteByte('=')
		aggregate.WriteTo(w)
	}
	w.WriteByte('}')
	w.WriteByte('\n')
	w.Flush()
}

type measure struct {
	Location string
	Temp     int32
}

type aggregate struct {
	Max   int32
	Min   int32
	Sum   int32
	Count int32
}

func (a *aggregate) Add(m measure) {
	if m.Temp > a.Max {
		a.Max = m.Temp
	}
	if m.Temp < a.Min {
		a.Min = m.Temp
	}
	a.Sum += m.Temp
	a.Count++
}

func (a *aggregate) Merge(b *aggregate) {
	if b.Max > a.Max {
		a.Max = b.Max
	}
	if b.Min < a.Min {
		a.Min = b.Min
	}
	a.Sum += b.Sum
	a.Count += b.Count
}

const FLOAT2INT = 10

type Writer interface {
	io.StringWriter
	io.ByteWriter
}

func (a *aggregate) WriteTo(w Writer) {
	w.WriteString(strconv.FormatFloat(float64(a.Min)/FLOAT2INT, 'f', 1, 64))
	w.WriteByte('/')
	w.WriteString(strconv.FormatFloat(a.Avg(), 'f', 1, 64))
	w.WriteByte('/')
	w.WriteString(strconv.FormatFloat(float64(a.Max)/FLOAT2INT, 'f', 1, 64))
}

func (a *aggregate) String() string {
	buf := bytes.Buffer{}
	a.WriteTo(&buf)
	return buf.String()
}

// rounding floats to 1 decimal place with 0.05 rounding up to 0.1
func round(x float64) float64 {
	return math.Floor((x+0.05)*10) / 10
}

func (a *aggregate) Avg() float64 {
	t := float64(a.Sum) / float64(a.Count*FLOAT2INT)
	return round(t)
}

func (m *measure) Parse(s string) {
	loc, tmp, ok := strings.Cut(s, ";")
	if !ok {
		log.Fatal("parse error, line: ", s)
	}
	val, err := strconv.ParseFloat(tmp, 32)
	if err != nil {
		log.Fatal("parse error, line: ", s, err)
	}

	m.Location = loc
	m.Temp = int32(round(val) * FLOAT2INT)
}
