package utils

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/pkg/errors"
)

const URL_STOCK_SPLIT_BARS = `https://api.polygon.io/v2/reference/splits/%s?&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

type StockSplit struct {
	Ticker string `json:"ticker"`
	ExDate string `json:"exDate"`
}

type StockSplitResult struct {
	StockSplits []StockSplit `json:"results"`
}

func GetStockSplit() {
	db, err := InitDB()
	if err != nil {
		log.Fatalln("Cannot init db", err)
	}
	defer db.Close()
	wp := workerpool.New(100)
	var allTickers []string
	ticker := os.Args[1]
	log.Println("=====",ticker,strings.Split(ticker,","))
	if ticker == "all" {
		allTickers, err = db.GetTickersFromDB()
		if err != nil {
			log.Println("Error when get all ticker", err)
			return
		}
	}else{
		allTickers= append(allTickers,strings.Split(ticker,",")... )
	}
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalln("Can't set timezone", err)
	}
	time.Local = loc // -> this is setting the global timezone
	log.Println("time=", time.Now())

	splitEnd, err := time.Parse("2006-01-02", os.Args[2])
	if err != nil {
		log.Fatalln("Can't parse time", err, os.Args[2], "Time must be in the format 2006-01-02")
	}

	f, err := os.OpenFile("tickerlog", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println("--------------------------------")
	log.Println("----------", splitEnd.Format("2006-01-02"), "----------")
	log.Println("--------------------------------")

	for _, ticker := range allTickers {
		ticker := ticker
		wp.Submit(func() {
			stockSplit, err := db.getStockSplits(ticker)
			if err != nil {
				log.Println("ERROR", ticker, err)
			}
			if stockSplit != nil {
				log.Println(ticker)
				latestDate, err := time.Parse("2006-01-02", stockSplit.ExDate)
				if err != nil {
					fmt.Println("unable to parse date")
				}

				// fmt.Println(*stockSplit, latestDate)
				// getEverything(ticker, db)

				if latestDate.After(splitEnd) && !latestDate.Equal(splitEnd) {
					fmt.Println(*stockSplit, latestDate, ticker)
					getEverything(ticker, db)
				}
			}
		})
	}
	wp.StopWait()

	log.Println("-----------------------")
	log.Println("----------END----------")
	log.Println("-----------------------")
}

func (db DB) getStockSplits(ticker string) (*StockSplit, error) {
	url := fmt.Sprintf(URL_STOCK_SPLIT_BARS, ticker)
	results := StockSplitResult{}
	err := getJson(url, &results)
	if err != nil {
		return &StockSplit{}, errors.Wrap(err, "can't get json "+url)
	}
	if len(results.StockSplits) > 0 {
		for _, stockSplit := range results.StockSplits {
			exDate, err := time.Parse("2006-01-02", stockSplit.ExDate)
			if err != nil {
				fmt.Println("unable to parse date")
			}
			if exDate.Before(time.Now()) {
				return &stockSplit, nil
			}
		}
	}
	return nil, nil
}

// TODO: dailybars duplicate
// TODO: check if oneminvol and v are correct

func getEverything(ticker string, db *DB) {
	updateDailybars(ticker, db)
	updateTickerChanges(ticker, db)
	updateTickerDuplicatesChanges(ticker, db)
	updateBreakouts(ticker, db)

	// updateHistorical(ticker, db)
	// updateLoBreakout(ticker, db)

	// updateOrderValues(ticker, db)

	updateAggregates(ticker, db)

}

func updateDailybars(ticker string, db *DB) {
	end := time.Now()
	// get latest date for dailybars
	rows, err := db.Query(`select min(date) from dailybars`)
	rows.Next()
	var start time.Time
	err = rows.Scan(&start)
	if err != nil {
		log.Println("unable to get the oldest date")
		return
	}

	_, err = db.Exec("DELETE FROM dailybars WHERE ticker=$1", ticker)
	if err != nil {
		log.Println("unable to delete ticker", ticker, err)
	}

	_, err = db.GetDailybarsData(ticker, start, end)
	if err != nil {
		log.Println("ERROR", ticker, err)
	}
}

func updateTickerChanges(ticker string, db *DB) {

	start := time.Now()
	// get latest date for dailybars
	rows, err := db.Query(`select min(date) from dailybars`)
	rows.Next()
	var end time.Time
	err = rows.Scan(&end)
	if err != nil {
		log.Println("unable to get the oldest date")
		return
	}

	for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
		if t.Weekday() == 0 || t.Weekday() == 6 {
			continue
		}
		err := db.updateChange(t.Format("2006-01-02"), []string{ticker})
		if err != nil {
			log.Println("UPDATE CHANGE ERROR", err)
		}
	}

}

func updateTickerDuplicatesChanges(ticker string, db *DB) {
	start := time.Now()
	// get latest date for dailybars
	rows, err := db.Query(`select min(date) from dailybars`)
	rows.Next()
	var end time.Time
	err = rows.Scan(&end)
	if err != nil {
		log.Println("unable to get the oldest date")
		return
	}

	_, err = db.Exec("delete from dailybars_duplicate where ticker=$1 and date>=$2", ticker, end)
	if err != nil {
		log.Println("Unable to delete from dailybars duplicate")
	}

	_, err = db.Exec("insert into dailybars_duplicate select * from dailybars where ticker=$1 and date>=$2", ticker, end)
	if err != nil {
		log.Println("Unable to insert into duplicates")
	}

	for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
		// t := t
		// wp.Submit(func() {
		if t.Weekday() == 0 || t.Weekday() == 6 {
			continue
		}
		err := db.updateChangeDuplicate(t.Format("2006-01-02"), []string{ticker})
		if err != nil {
			log.Println("UPDATE CHANGE ERROR", err)
		}
		// })

	}
}

var dailyBarsUpdates = struct {
	sync.RWMutex
	m map[string][2000]tickerData
}{
	m: make(map[string][2000]tickerData),
}

func updateBreakouts(ticker string, db *DB) {
	start := time.Now()
	// get latest date for dailybars
	rows, err := db.Query(`select min(date) from dailybars`)
	rows.Next()
	var end time.Time
	err = rows.Scan(&end)
	if err != nil {
		log.Println("unable to get the oldest date")
		return
	}

	_, err = db.Exec("DELETE FROM breakout WHERE ticker=$1 and date>=$2", ticker, end)
	if err != nil {
		log.Println("unable to delete ticker", ticker, err)
	}

	dailyBarsUpdates.m = make(map[string][2000]tickerData)

	log.Println("loading", ticker)
	var data [2000]tickerData
	rowsx, err := db.Queryx("SELECT to_char(date, 'YYYY-MM-DD') as date,h,v,oneminvol FROM dailybars WHERE ticker=$1 AND date<$2 ORDER BY date DESC limit 2000", ticker, start)
	if err != nil {
		log.Println(err, "ERROR loadAllTickersData SELECT")
	}
	var i int
	for rowsx.Next() {
		var tmp tickerData
		err := rowsx.StructScan(&tmp)
		if err != nil {
			log.Println(err, "ERROR loadAlltickerData StructScan")
		}
		data[i] = tmp
		i++
	}
	dailyBarsUpdates.Lock()
	dailyBarsUpdates.m[ticker] = data
	dailyBarsUpdates.Unlock()

	dates := []string{}

	raws, err := db.Query(`select distinct(date) from dailybars where date>$1 and date<$2 order by date desc`, end.Format("2006-01-02"), start)
	if err != nil {
		log.Fatalln(err, "ERROR select date")
	}
	for raws.Next() {
		var t time.Time
		err := raws.Scan(&t)
		if err != nil {
			log.Fatalln(err, "ERROR SCAN updatechangeall")
		}
		dates = append(dates, t.Format("2006-01-02"))
	}

	for _, d := range dates {
		log.Println("STARTING", d)
		db.findBreakoutsUpdates(d, ticker)
	}
}

func updateHistorical(ticker string, db *DB) {
	start := time.Now()
	// get latest date for dailybars
	rows, err := db.Query(`select min(date) from largestorders`)
	rows.Next()
	var end time.Time
	err = rows.Scan(&end)
	if err != nil {
		log.Println("unable to get the oldest date")
		return
	}

	_, err = db.Exec("DELETE FROM largestorders WHERE ticker=$1 and date>=$2", ticker, end)
	if err != nil {
		log.Println("unable to delete Ticker", ticker, err)
	}

	_, err = db.Exec("DELETE FROM averages WHERE ticker=$1 and date>=$2", ticker, end)
	if err != nil {
		log.Println("unable to delete ticker", ticker, err)
	}

	for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
		if t.Weekday() == 0 || t.Weekday() == 6 {
			continue
		}
		log.Println("GETTRADES", ticker, t)
		db.getTrades(ticker, t)

	}
}

var largestOrdersUpdates = struct {
	sync.RWMutex
	m map[string][2000]loData
}{
	m: make(map[string][2000]loData),
}

func updateLoBreakout(ticker string, db *DB) {
	start := time.Now()
	// get latest date for dailybars
	rows, err := db.Query(`select min(date) from largestorders`)
	rows.Next()
	var end time.Time
	err = rows.Scan(&end)
	if err != nil {
		log.Println("unable to get the oldest date")
		return
	}

	_, err = db.Exec("DELETE FROM largestorders_breakout WHERE ticker=$1 and date>=$2", ticker, end)
	if err != nil {
		log.Println("unable to delete ticker", ticker, err)
	}

	largestOrdersUpdates.m = make(map[string][2000]loData)

	log.Println("loading", ticker)
	var data [2000]loData
	rowsx, err := db.Queryx("SELECT to_char(date, 'YYYY-MM-DD') as date, s FROM largestorders WHERE ticker=$1 AND date<=$2 ORDER BY date DESC limit 610", ticker, start)
	if err != nil {
		log.Println(err, "ERROR loadAllTickersData SELECT")
	}
	var i int
	for rowsx.Next() {
		var tmp loData
		err := rowsx.StructScan(&tmp)
		if err != nil {
			log.Println(err, "ERROR loadAllloData StructScan")
		}
		data[i] = tmp
		i++
	}
	largestOrdersUpdates.Lock()
	largestOrdersUpdates.m[ticker] = data
	largestOrdersUpdates.Unlock()

	dates := []string{}

	raws, err := db.Query(`select distinct(date) from largestorders where date>$1 and date<$2 order by date desc`, end.Format("2006-01-02"), start)
	if err != nil {
		log.Fatalln(err, "ERROR select date")
	}

	for raws.Next() {
		var t time.Time
		err := raws.Scan(&t)
		if err != nil {
			log.Fatalln(err, "ERROR SCAN updatechangeall")
		}
		dates = append(dates, t.Format("2006-01-02"))
	}

	for _, d := range dates {
		_, err = db.Exec("DELETE FROM largestorders_breakout WHERE date=$1 and ticker=$2", d, ticker)
		if err != nil {
			log.Fatalln("ERROR Cannot delete daily bars", err)
		}
		log.Println("STARTING", d)
		db.findLargestordersBreakoutsUpdates(d, ticker)
	}
}

func updateOrderValues(ticker string, db *DB) {
	start := time.Now()
	// get latest date for dailybars
	rows, err := db.Query(`select min(date) from dailybars`)
	rows.Next()
	var end time.Time
	err = rows.Scan(&end)
	if err != nil {
		log.Println("unable to get the oldest date")
		return
	}

	_, err = db.Exec("DELETE FROM order_value WHERE ticker=$1 and date>=$2", ticker, end)
	if err != nil {
		log.Println("unable to delete ticker", ticker, err)
	}

	log.Println("START WORKER", ticker)
	for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
		if t.Weekday() == 0 || t.Weekday() == 6 {
			continue
		}
		log.Println("Update lo", ticker, t)
		UpdateLoValue(t, ticker, db)

	}
	log.Println("END WORKER", ticker)

}

func updateAggregates(ticker string, db *DB) {
	start := time.Now()
	// get latest date for dailybars
	rows, err := db.Query(`select min(date) from dailybars`)
	rows.Next()
	var end time.Time
	err = rows.Scan(&end)
	if err != nil {
		log.Println("unable to get the oldest date")
		return
	}

	_, err = db.Exec("delete from aggregates where ticker=$1", ticker)
	if err != nil {
		log.Println("Unable to delete from aggregates", err)
	}

	_, err = db.Exec(`INSERT INTO aggregates select d.ticker, d.date, d.c as c_x, d.o as o, d.change, d.v as v, ddup.change1, ddup.change2, ddup.change3, ddup.change4, ddup.change5, ddup.change6, ddup.change7, ddup.change8, ddup.change9, ddup.change10, ddup.change11, ddup.change12, ddup.change13, ddup.change14, b.h as h_y, b.v as v_y, b.oneminvol as oneminvol_y, av.avg, av.stddev, av.count, ov.day_ratio_15, 
	(select sum(v.s) as Lo_breakout from (select * from largestorders_breakout where date>=d.date - INTERVAL '15 day' and date<=d.date and ticker=d.ticker order by date desc limit 5) as v) as Lo_breakout
	from largestorders lo 
	inner join breakout b on lo.ticker=b.ticker and lo.date=b.date 
	inner join dailybars d on b.ticker=d.ticker and b.date=d.date  
	inner join averages av on d.ticker=av.ticker and av.date=d.date 
	inner join order_value ov on av.ticker=ov.ticker and ov.date=av.date 
	inner join dailybars_duplicate ddup on ov.ticker=ddup.ticker and ov.date=ddup.date where d.date>=$1 and d.date<=$2 and d.ticker=$3`, end, start, ticker)
	if err != nil {
		log.Println(err)
	}
}
