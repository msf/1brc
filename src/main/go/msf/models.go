package main

import (
	"bufio"
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

func NewAllMeasures() *AllMeasures {
	return &AllMeasures{Locations: make(map[string]*aggregate)}
}

func (a *AllMeasures) ReadReadings(file *os.File) {
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var m measure
		s := scanner.Text()
		m.Parse(s)
		a.Add(m)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func (a *AllMeasures) Add(m measure) {
	if rec, ok := a.Locations[m.Location]; !ok {
		a.Locations[m.Location] = &aggregate{Max: m.Temp, Min: m.Temp, Sum: m.Temp, Count: 1}
	} else {
		rec.Add(m)
	}
}

func (a *AllMeasures) Print(dst io.Writer) {
	sortedLocations := make([]string, 0, len(a.Locations))
	for location := range a.Locations {
		sortedLocations = append(sortedLocations, location)
	}
	sort.Strings(sortedLocations)

	w := bufio.NewWriter(dst)

	buf := []byte{'{', '=', '/', '}', ',', ' ', '\n'}
	w.WriteByte(buf[0])
	for i, location := range sortedLocations {
		aggregate := a.Locations[location]
		if i > 0 {
			w.WriteByte(buf[4])
			w.WriteByte(buf[5])
		}
		w.WriteString(location)
		w.WriteByte(buf[1])
		w.WriteString(strconv.FormatFloat(float64(aggregate.Min), 'f', 1, 32))
		w.WriteByte(buf[2])
		w.WriteString(strconv.FormatFloat(float64(aggregate.Avg()), 'f', 1, 32))
		w.WriteByte(buf[2])
		w.WriteString(strconv.FormatFloat(float64(aggregate.Max), 'f', 1, 32))
	}
	w.WriteByte(buf[3])
	w.WriteByte(buf[6])
	w.Flush()
}

type measure struct {
	Location string
	Temp     float32
}

type aggregate struct {
	Max   float32
	Min   float32
	Sum   float32
	Count uint32
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

// rounding floats to 1 decimal place with 0.05 rounding up to 0.1
func round(x float64) float64 {
	return math.Floor((x+0.05)*10) / 10
}

func (a *aggregate) Avg() float32 {
	return float32(round(float64(a.Sum) / float64(a.Count)))
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
	m.Temp = float32(val)
}
