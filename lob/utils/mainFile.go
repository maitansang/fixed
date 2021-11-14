package utils

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/paulbellamy/ratecounter"
	"github.com/pkg/errors"
)

type ins struct {
	Ticker string
	Date   string
	S      int
}
type tickerData struct {
	Ticker string `db:"ticker"`
	Date   string `db:"date"`
	S      int    `db:"s"`
}
type datetickerinfo struct {
	Info map[string]*tickerData
}

var dailyBars = struct {
	sync.RWMutex
	m     map[string][]tickerData
	dates map[string]datetickerinfo
}{
	m: make(map[string][]tickerData),
}

var counter *ratecounter.RateCounter
var insert chan *ins
var debug = false

func InitDB() (*DB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 user=postgres dbname=stockmarket password=P`AgD!9g!%~hz3M<	sslmode=disable")
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}
	d := &DB{
		db,
	}

	return d, nil
}

type DB struct {
	*sqlx.DB
}

func MainFunc() {
	counter = ratecounter.NewRateCounter(30 * time.Second)
	db, err := InitDB()
	if err != nil {
		log.Fatalln("Cannot init db", err)
	}
	defer db.Close()
	insert = make(chan *ins, 15000)
	db.SetMaxOpenConns(150)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(60 * time.Minute)
	//var tickers []string
	db.loadDailyBarsMem()

}

func (db DB) loadDailyBarsMem() {
	tickers, err := db.GetTickersFromDB()
	if err != nil {
		log.Fatalln("error getting tickers", err)
	}
	// tickers = []string{"AAPL"}
	log.Println("START updateDailyBarsMem")

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalln("Can't set timezone", err)
	}
	time.Local = loc

	// start := time.Now()
	// load daily all bars  starting from yesterday
	//  first delete previous dailyBars
	dailyBars.m = make(map[string][]tickerData)
	dailyBars.dates = make(map[string]datetickerinfo)
	// load the new ones
	// endDate := time.Now()
	endDate, _ := time.Parse("2006-01-02", os.Args[2])
	startDate, _ := time.Parse("2006-01-02", os.Args[1])
	startFrom := startDate.AddDate(0, 0, -254)
	log.Println("startDate:", startDate, "endDate:", endDate, "startFrom", startFrom)

	// tickers = []string{"AAPL"}

	//
	wpool := workerpool.New(16)
	wploadbars := workerpool.New(50)
	inserter := workerpool.New(20)

	for i := 0; i < 20; i++ {
		inserter.Submit(func() {
			log.Println("Channel open for values")
			list := make([]*ins, 0)
			for x := range insert {
				counter.Incr(1)
				list = append(list, x)
				if len(list) > 2000 {

					err := db.doInsert(list)
					log.Println("Inserting", len(list), "values into largestorders_breakout")
					// _, err = db.NamedExec(`INSERT INTO largestorders_breakout (date,ticker,s)
					// 	VALUES(:date, :ticker, :s)`, list)
					// cnt := len(list)
					list = make([]*ins, 0)
					if err != nil {
						// spew.Dump("cnt", cnt)
						log.Println(err, "ERROR SCAN updatechangeall")
					}
				}
			}
			if len(list) > 0 {
				log.Println("Inserting", len(list), "values into largestorders_breakout")
				// _, err = db.NamedExec(`INSERT INTO largestorders_breakout (date,ticker,s)
				// 			VALUES(:date, :ticker, :s)`, list)

				err := db.doInsert(list)
				if err != nil {
					// spew.Dump(list)
					log.Println(err, "ERROR SCAN updatechangeall")
				}
			}
			log.Println("Channel closed for values")
		})
	}
	for _, ticker := range tickers {
		ticker := ticker
		wploadbars.Submit(func() {

			// dateinfo := &datetickerinfo{
			// 	Info: make(map[string]*tickerData),
			// }
			log.Println("loading", ticker)
			var data []tickerData
			rows, err := db.Queryx("SELECT to_char(date, 'YYYY-MM-DD') as date,s,ticker FROM largestorders WHERE ticker=$1 AND date<=$2 AND date>=$3 ORDER BY date DESC", ticker, endDate, startFrom)

			if err != nil {
				log.Println(err, "ERROR loadAllTickersData SELECT")
			}
			var i int
			for rows.Next() {
				var tmp tickerData
				err := rows.StructScan(&tmp)
				if err != nil {
					log.Println(err, "ERROR loadAlltickerData StructScan")
				}
				// spew.Dump("tmp", tmp)
				// data[i] = tmp
				data = append(data, tmp)
				// dateinfo.Info[tmp.Date] = &tmp
				i++
			}
			wpool.Submit(func() { db.checkBreakouts(ticker, data, startDate, endDate) })

			// dailyBars.Lock()
			// dailyBars.m[ticker] = data
			// dailyBars.dates[ticker] = *dateinfo
			// dailyBars.Unlock()
		})
	}
	wploadbars.StopWait()
	log.Println("wploadbars done")
	wpool.StopWait()
	log.Println("wpool done")
	close(insert)
	log.Println("close(insert) done")
	inserter.StopWait()
	log.Println("inserter done")

	// log.Println("FINISHED updateDailyBarsMem")
	// // fmt.Println("dailybars", dailyBars.m["AAPL"])
	// log.Println("Time to load dailybars", time.Since(start))

	// dates := []string{}

	// // endDate := time.Now().AddDate(-1, 0, 0)
	// // endDate, _ := time.Parse("2006-01-02", "2019-01-01")

	// raws, err := db.Query(`select distinct(date) from largestorders_breakout where date>$1 and date<=$2 order by date DESC`, endDate.Format("2006-01-02"), endDate)
	// if err != nil {
	// 	log.Fatalln(err, "ERROR select date")
	// }
	// for raws.Next() {
	// 	var t time.Time
	// 	err := raws.Scan(&t)
	// 	if err != nil {
	// 		log.Fatalln(err, "ERROR SCAN updatechangeall")
	// 	}
	// 	dates = append(dates, t.Format("2006-01-02"))
	// }

	// for _, d := range dates {
	// 	log.Println("STARTING", d)
	// 	db.findBreakouts(d)
	// }
	// newwp.StopWait()
}

// type resScan struct {
// 	Date      time.Time `db:"date"`
// 	Ticker    string    `db:"ticker"`
// 	H         float64   `db:"h"`
// 	V         int64     `db:"v"`
// 	Oneminvol int64     `db:"oneminvol"`
// }

// type res struct {
// 	Date      string  `db:"date"`
// 	Ticker    string  `db:"ticker"`
// 	H         float64 `db:"h"`
// 	V         int64   `db:"v"`
// 	Oneminvol int64   `db:"oneminvol"`
// }

// func (db DB) findBreakouts(date string) {
// 	// // rows, err := db.Queryx("SELECT date,ticker,h,v,oneminvol FROM largestorders_breakout WHERE date=$1", date)
// 	// if err != nil {
// 	// 	log.Fatalln("CANNOT SELECT", err)
// 	// }
// 	// var bars []res
// 	// for rows.Next() {
// 	// 	var r resScan
// 	// 	err := rows.StructScan(&r)
// 	// 	if err != nil {
// 	// 		log.Fatalln("CANNOT STRUCTSCAN", err)
// 	// 	}
// 	// 	bars = append(bars, res{
// 	// 		Date:      r.Date.Format("2006-01-02"),
// 	// 		Ticker:    r.Ticker,
// 	// 		H:         r.H,
// 	// 		V:         r.V,
// 	// 		Oneminvol: r.Oneminvol,
// 	// 	})
// 	// }
// 	var bars []tickerData
// 	for _, info := range dailyBars.dates {
// 		if val, ok := info.Info[date]; ok {
// 			bars = append(bars, *val)
// 		}
// 	}
// 	// spew.Dump("bars", bars, dailyBars.dates)
// 	for _, b := range bars {
// 		db.findBreakoutsOne(b)
// 	}

// }
func (db DB) checkBreakouts(ticker string, arr []tickerData, start, end time.Time) {
	log.Println("Find Breakouts", ticker)
	for _, b := range arr {
		d, _ := time.Parse("2006-01-02", b.Date)
		if d.Before(start) || end.Before(d) {
			// log.Println(start.Format("2006-01-02"), ":::", b.Date, ":::", end.Format("2006-01-02"))
			continue // out of range
		}
		db.findBreakoutsOne(b, arr)
	}
}

func (db DB) findBreakoutsOne(bar tickerData, data []tickerData) {
	//log.Println("BAR=", bar)
	// data, ok := dailyBars.m[bar.Ticker]
	// if !ok {
	// 	log.Println("No daily bars found for", bar.Ticker)
	// 	return
	// }
	//log.Println("DATA=", data)
	date := bar.Date
	var sp int
	for sp = 0; sp < len(data)-1; sp++ {
		// log.Println(date, data[sp].Date, sp)
		if data[sp].Date == date {
			break
		}
	}
	// if i > 225 {
	// 	log.Println("DATA NOT FOUND", bar)
	// 	return
	// }

	var sBr int

	dateNow, err := time.Parse("2006-01-02", bar.Date)
	if err != nil {
		log.Println("Invalid date", bar, err)
	}
	before1Yr := dateNow.AddDate(0, 0, -255)

	for i := sp + 1; i < len(data) && i < sp+255; i++ {
		// log.Println("iii", len(data[sp:]), i)
		d := data[i]
		// if d.Date >= bar.Date {
		// 	continue
		// }

		if debug {
			log.Println(bar.Date, d.Date, sp, "::::", "hBr", d.S, ">", bar.S, ":::", sBr)
		}
		barDate, err := time.Parse("2006-01-02", d.Date)
		if err != nil {
			log.Println("INVALID TIME", d)
			break
		}
		if barDate.Before(before1Yr) {
			// log.Println("before", barDate.Format("2006-01-02"), before1Yr.Format("2006-01-02"))
			break
		}
		if d.S >= bar.S {
			// log.Println("high", d.High, ">=", bar.High)
			break
		}
		if sBr > 254 {
			// log.Println("254")
			break
		}
		sBr++
	}
	insert <- &ins{
		Ticker: bar.Ticker,
		Date:   bar.Date,
		S:      sBr,
	}
	log.Println("Breakout found", bar.Ticker, bar.Date, sBr, "RPS:", counter.Rate()/30)
	// log.Println("Breakout found", bar.Ticker, bar.Date, hBr, vBr, oneMBr, "bar.High:", bar.High, "bar.Vol:", bar.Vol, "bar.Oneminvol:", bar.Oneminvol)

}

func (db DB) doInsert(x []*ins) error {
	q := "INSERT INTO largestorders_breakout (date,ticker,s) VALUES"

	for _, v := range x {
		q += fmt.Sprintf("('%s','%s','%d'),", v.Date, v.Ticker, v.S)
	}
	q = q[0 : len(q)-1]
	q = q + "ON CONFLICT (ticker,date) DO UPDATE SET s = excluded.s;"
	_, err := db.Exec(q)
	return err
	// log.Println("Inserting", len(list), "values into breakout")
	// _, err = db.NamedExec(`INSERT INTO breakout (date,ticker,h,v,oneminvol)
	// 	VALUES(:date, :ticker, :hbr, :vbr, :onembr) ON CONFLICT (ticker,date) DO NOTHING`, list)
	// // cnt := len(list)
}
