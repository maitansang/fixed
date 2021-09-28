package utils

import (
	"log"
	"math"
	"os"
	"time"

	"github.com/gammazero/workerpool"
)

func MainFunc() {
	var tickers []string
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalln("Can't set timezone", err)
	}
	time.Local = loc // -> this is setting the global timezone
	log.Println("time=", time.Now())

	if len(os.Args) < 2 {
		log.Fatalln("Please provide start date")
	}

	db, err := InitDB()
	if err != nil {
		log.Fatalln("Can't init db", err)
	}

	defer db.Close()

	start, err := time.Parse("2006-01-02", os.Args[1])
	if err != nil {
		log.Fatalln("Can't parse time", err, os.Args[1], "Time must be in the format 2006-01-02")
	}
	if len(os.Args) > 3 {
		tickerInput := os.Args[3]

		checkExistTiker, err := db.CheckTickerFromDB(tickerInput)
		if err != nil {
			log.Fatalln(err)
		}
		if checkExistTiker {
			tickers = []string{tickerInput}
		} else {
			tickers, err = db.GetTickersFromDB()
			if err != nil {
				log.Fatalln("Can not get tickers from db", err)
			}
		}
	} else {
		tickers, err = db.GetTickersFromDB()
		if err != nil {
			log.Fatalln("Can not get tickers from db", err)
		}
	}

	end, err := time.Parse("2006-01-02", os.Args[2])
	if err != nil {
		log.Fatalln("Can't parse time", err, os.Args[2], "Time must be in the format 2006-01-02")
	}
	for t := start.AddDate(0, 0, +1); t.Before(end) || t.Equal(end); t = t.AddDate(0, 0, +1) {
		if t.Weekday() == 0 || t.Weekday() == 6 {
			continue
		}
		err := db.updateChange(t.Format("2006-01-02"), tickers)
		if err != nil {
			log.Println("UPDATE CHANGE ERROR", err)
		}
	}
}

type line struct {
	date  time.Time
	high  float64
	close float64
}

func (db DB) updateChange(date string, tickers []string) error {
	//tickers = getTickers()
	//log.Println(t)

	wpUpdate := workerpool.New(192)
	for i, ticker := range tickers {
		ticker := ticker
		wpUpdate.Submit(func() {
			//fmt.Println(tickers)
			//ticker := tickers[i]

			log.Println("updating", i, ticker, date)
			var lines []line
			raws, err := db.Query(`select date,h,c from dailybars where ticker=$1 and date<=$2 order by date desc limit 2`, ticker, date)
			if err != nil {
				// return errors.Wrap(err, "ERROR SELECT updatechange")
				log.Println("error select", err)
				return
			}
			_, _ = raws, lines
			for raws.Next() {
				var l line
				err = raws.Scan(&l.date, &l.high, &l.close)
				if err != nil {
					log.Println(err, "ERROR SCAN updatechange")
					// continue errors.Wrap(err, "ERROR SCAN updatechange")
					log.Println("Error Scan", err)
					continue
				}
				lines = append(lines, l)
			}
			if len(lines) < 2 {
				log.Println("ERRORORORO", len(lines), ticker, date)
				return
				//continue errors.New("ERROR updatechange NOT ENOUGH RAWS " + ticker)
			}
			change := (lines[0].high - lines[1].close) / lines[1].close * 100
			_, err = db.Exec(`UPDATE dailybars SET change=$1 WHERE date=$2 AND ticker=$3`, change, lines[0].date.Format("2006-01-02"), ticker)
			if err != nil {
				// continue errors.Wrap(err, "ERROR updatechange CANNOT UPDATE "+lines[0].date.Format("2006-01-02")+" "+ticker)
				log.Println("error can not update", err)
				_, err = db.Exec(`UPDATE dailybars SET change=$1 WHERE date=$2 AND ticker=$3`, nil, lines[0].date.Format("2006-01-02"), ticker)
				if err != nil {
					// continue errors.Wrap(err, "ERROR updatechange CANNOT UPDATE "+lines[0].date.Format("2006-01-02")+" "+ticker)
					log.Fatalln("error can not update", err)
					return
				}
				return
			}
		})
	}
	wpUpdate.StopWait()
	return nil
}

func conv2DecDigits(x float64) float64 {
	return math.Round(x*100) / 100
}
