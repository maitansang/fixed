package utils

import (
	"fmt"
	"math"
	"sort"
	"time"

	"gonum.org/v1/gonum/stat"
)

const (
	columnX = `x`
	columnP = `p`
	columnS = `s`
	columnZ = `z`
	columnC = `c`

	firstQuartile  = 0.25
	secondQuartile = 0.5
	thirdQuartile  = 0.75
)

type TradeFeatures struct {
	Ticker string
	Column string
	Date   string
	Count  int64
	Unique float64
	Top    string
	Freq   float64
	Mean   float64
	StdDev float64
	Min    float64
	Q1     float64
	Q2     float64
	Q3     float64
	Max    float64
	Sum    float64
}

func (db DB) extractTradesFeatures(ticker string, in []Result) (out []TradeFeatures) {
	resMap := make(map[string][]Result) // map[date][]OldResult
	out = make([]TradeFeatures, 0)
	for index := range in {
		res := in[index]
		date := time.Unix(res.T/1000000000, 0).Format("2006-01-02")
		rec, ok := resMap[date]
		if !ok {
			rec = make([]Result, 0)
		}
		rec = append(rec, res)
		resMap[date] = rec
	}

	for d, v := range resMap {
		out = append(out, calculateFeatures(ticker, d, v)...)
	}

	return out
}

type mins []float64

func (m mins) updateMins(rec Result) {
	if float64(rec.X) < m[0] {
		m[0] = float64(rec.X)
	}
	if float64(rec.P) < m[1] {
		m[1] = float64(rec.P)
	}
	if float64(rec.Z) < m[2] {
		m[2] = float64(rec.Z)
	}
	if float64(rec.S) < m[3] {
		m[3] = float64(rec.S)
	}
}

func (m mins) initialize(rec Result) {
	m[0] = float64(rec.X)
	m[1] = rec.P
	m[2] = float64(rec.Z)
	m[3] = float64(rec.S)
}

func (m mins) Min(column string) float64 {
	switch column {
	case columnX:
		return m[0]
	case columnP:
		return m[1]
	case columnZ:
		return m[2]
	case columnS:
		return m[3]
	default:
		return 0
	}
}

func newMinMax() (mins, maxs) {
	return make(mins, 4), make(maxs, 4)
}

type maxs []float64

func (m maxs) updateMaxs(rec Result) {
	if float64(rec.X) > m[0] {
		m[0] = float64(rec.X)
	}
	if float64(rec.P) > m[1] {
		m[1] = float64(rec.P)
	}
	if float64(rec.Z) > m[2] {
		m[2] = float64(rec.Z)
	}
	if float64(rec.S) > m[3] {
		m[3] = float64(rec.S)
	}
}

func (m maxs) Max(column string) float64 {
	switch column {
	case columnX:
		return m[0]
	case columnP:
		return m[1]
	case columnZ:
		return m[2]
	case columnS:
		return m[3]
	default:
		return 0
	}
}

func arrToStr(in []int) string {
	if len(in) == 0 {
		return "[]"
	}
	return fmt.Sprintf("%v", in)
}

func calculateFeatures(ticker string, date string, in []Result) []TradeFeatures {
	var count int64
	var sumX, sumC float64
	var calcInX, calcInP, calcInS, calcInZ []float64
	var mins, maxs = newMinMax()
	var mappedC = make(map[string]int64)
	var listCalcX = make(map[int64][]float64)
	var listCalcC = make(map[int][]float64)

	for index := range in {
		rec := in[index]
		if count == 0 {
			mins.initialize(rec)
		}
		calcInX = append(calcInX, float64(rec.X))
		calcInZ = append(calcInZ, float64(rec.Z))
		calcInP = append(calcInP, rec.P)
		calcInS = append(calcInS, float64(rec.S))
		listCalcX[rec.X] = append(listCalcX[rec.X], float64(rec.S))
		for _, v := range rec.C {
			listCalcC[v] = append(listCalcC[v], float64(rec.S))
		}
		mappedC[arrToStr(rec.C)] += 1
		count++
		mins.updateMins(rec)
		maxs.updateMaxs(rec)
	}

	for _, v := range listCalcX {
		sort.Slice(v, func(i, j int) bool {
			return v[i] < v[j]
		})
	}
	for _, v := range listCalcC {
		sort.Slice(v, func(i, j int) bool {
			return v[i] < v[j]
		})
	}
	sort.Slice(calcInX, func(i, j int) bool {
		return calcInX[i] < calcInX[j]
	})

	sort.Slice(calcInP, func(i, j int) bool {
		return calcInP[i] < calcInP[j]
	})

	sort.Slice(calcInS, func(i, j int) bool {
		return calcInS[i] < calcInS[j]
	})

	sort.Slice(calcInZ, func(i, j int) bool {
		return calcInZ[i] < calcInZ[j]
	})

	//Get all of s value
	for _, v := range listCalcX {
		for _, p := range v {
			sumX += p
		}
	}
	for _, v := range listCalcC {
		for _, p := range v {
			sumC += p
		}
	}

	tf := make([]TradeFeatures, 0)
	tf = append(tf, TradeFeatures{
		Ticker: ticker,
		Column: columnX,
		Date:   date,
		Count:  count,
		Unique: math.NaN(),
		Top:    "NaN",
		Freq:   math.NaN(),
		Mean:   stat.Mean(calcInX, nil),
		StdDev: stat.StdDev(calcInX, nil),
		Min:    mins.Min(columnX),
		Q1:     stat.Quantile(firstQuartile, stat.Empirical, calcInX, nil),
		Q2:     stat.Quantile(secondQuartile, stat.Empirical, calcInX, nil),
		Q3:     stat.Quantile(thirdQuartile, stat.Empirical, calcInX, nil),
		Max:    maxs.Max(columnX),
		Sum:    sumX,
	}, TradeFeatures{
		Ticker: ticker,
		Column: columnP,
		Date:   date,
		Count:  count,
		Unique: math.NaN(),
		Top:    "NaN",
		Freq:   math.NaN(),
		Mean:   stat.Mean(calcInP, nil),
		StdDev: stat.StdDev(calcInP, nil),
		Min:    mins.Min(columnP),
		Q1:     stat.Quantile(firstQuartile, stat.Empirical, calcInP, nil),
		Q2:     stat.Quantile(secondQuartile, stat.Empirical, calcInP, nil),
		Q3:     stat.Quantile(thirdQuartile, stat.Empirical, calcInP, nil),
		Max:    maxs.Max(columnP),
		Sum:    math.NaN(),
	}, TradeFeatures{
		Ticker: ticker,
		Column: columnZ,
		Date:   date,
		Count:  count,
		Unique: math.NaN(),
		Top:    "NaN",
		Freq:   math.NaN(),
		Mean:   stat.Mean(calcInZ, nil),
		StdDev: stat.StdDev(calcInZ, nil),
		Min:    mins.Min(columnZ),
		Q1:     stat.Quantile(firstQuartile, stat.Empirical, calcInZ, nil),
		Q2:     stat.Quantile(secondQuartile, stat.Empirical, calcInZ, nil),
		Q3:     stat.Quantile(thirdQuartile, stat.Empirical, calcInZ, nil),
		Max:    maxs.Max(columnZ),
		Sum:    math.NaN(),
	}, TradeFeatures{
		Ticker: ticker,
		Column: columnS,
		Date:   date,
		Count:  count,
		Unique: math.NaN(),
		Top:    "NaN",
		Freq:   math.NaN(),
		Mean:   stat.Mean(calcInS, nil),
		StdDev: stat.StdDev(calcInS, nil),
		Min:    mins.Min(columnS),
		Q1:     stat.Quantile(firstQuartile, stat.Empirical, calcInS, nil),
		Q2:     stat.Quantile(secondQuartile, stat.Empirical, calcInS, nil),
		Q3:     stat.Quantile(thirdQuartile, stat.Empirical, calcInS, nil),
		Max:    maxs.Max(columnS),
		Sum:    math.NaN(),
	})

	var topC string
	var freqC, uniqC int64
	for k, v := range mappedC {
		if v > freqC {
			topC = k
			freqC = v
		}
		uniqC++
	}

	tf = append(tf, TradeFeatures{
		Ticker: ticker,
		Column: columnC,
		Date:   date,
		Count:  count,
		Unique: float64(uniqC),
		Top:    topC,
		Freq:   float64(uniqC),
		Mean:   math.NaN(),
		StdDev: math.NaN(),
		Min:    math.NaN(),
		Q1:     math.NaN(),
		Q2:     math.NaN(),
		Q3:     math.NaN(),
		Max:    math.NaN(),
		Sum:    sumC,
	})
	for i, v := range listCalcX {
		if i > 20 {
			continue
		}
		columnx := fmt.Sprintf("x%d", i)
		tf = append(tf, TradeFeatures{
			Ticker: ticker,
			Column: columnx,
			Date:   date,
			Count:  int64(len(v)),
			Unique: math.NaN(),
			Top:    "NaN",
			Freq:   math.NaN(),
			Mean:   stat.Mean(v, nil),
			StdDev: stat.StdDev(v, nil),
			Min:    mins.Min(columnx),
			Q1:     stat.Quantile(firstQuartile, stat.Empirical, v, nil),
			Q2:     stat.Quantile(secondQuartile, stat.Empirical, v, nil),
			Q3:     stat.Quantile(thirdQuartile, stat.Empirical, v, nil),
			Max:    maxs.Max(columnx),
			Sum:    math.NaN(),
		})
	}
	for i, v := range listCalcC {
		if i > 65 {
			continue
		}
		columnc := fmt.Sprintf("c%d", i)
		tf = append(tf, TradeFeatures{
			Ticker: ticker,
			Column: columnc,
			Date:   date,
			Count:  int64(len(v)),
			Unique: math.NaN(),
			Top:    "NaN",
			Freq:   math.NaN(),
			Mean:   stat.Mean(v, nil),
			StdDev: stat.StdDev(v, nil),
			Min:    mins.Min(columnc),
			Q1:     stat.Quantile(firstQuartile, stat.Empirical, v, nil),
			Q2:     stat.Quantile(secondQuartile, stat.Empirical, v, nil),
			Q3:     stat.Quantile(thirdQuartile, stat.Empirical, v, nil),
			Max:    maxs.Max(columnc),
			Sum:    math.NaN(),
		})
	}
	return tf
}
