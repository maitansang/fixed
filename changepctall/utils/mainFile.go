package utils

import (
	"errors"
	"log"
	"math"
	"os"
	"reflect"
	"time"

	"github.com/gammazero/workerpool"
)

func MainFunc() {
	loc, err := time.LoadLocation("EST")
	var tickers []string
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
	start = start.AddDate(0, 0, +1)
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
		}
	} else {
		tickers, err = db.GetTickersFromDB()
	}
	// tickers := []string{"AAPL"}
	if err != nil {
		log.Fatalln("Cant get tickers", err)
	}

	// wp := workerpool.New(100)

	// end := start.AddDate(-1, 0, 0)
	end, _ := time.Parse("2006-01-02", os.Args[2])
	if err != nil {
		log.Fatalln("Can't parse time", err, os.Args[2], "Time must be in the format 2006-01-02")
	}
	db.updateDailybarsDuplicates(tickers, start.Format("2006-01-02"), end.Format("2006-01-02"))

	for t := start; t.Before(end) || t.Equal(end); t = t.AddDate(0, 0, +1) {
		// t := t
		// wp.Submit(func() {
		if t.Weekday() == 0 || t.Weekday() == 6 {
			continue
		}
		err := db.updateChange(t.Format("2006-01-02"), tickers)
		if err != nil {
			log.Println("UPDATE CHANGE ERROR", err)
		}
		// })

	}
	// wp.StopWait()
}

type line struct {
	date  time.Time
	high  float64
	close float64
}

func (db *DB) updateDailybarsDuplicates(tickers []string, start string, end string) {
	wpUpdateDuplicates := workerpool.New(100)
	for _, ticker := range tickers {
		ticker := ticker
		wpUpdateDuplicates.Submit(func() {
			_, err := db.Exec("delete from dailybars_duplicate where ticker=$1 and date>=$2 and date<=$3", ticker, start, end)
			if err != nil {
				log.Println("unable to delete from duplicates")
				return
			}

			_, err = db.Exec("insert into dailybars_duplicate select * from dailybars where ticker=$1 and date>=$2 and date<=$3", ticker, start, end)
			if err != nil {
				log.Println("Unable to insert into duplicates")
			}
		})
	}
	wpUpdateDuplicates.StopWait()
}

func (db DB) updateChange(date string, tickers []string) error {
	//tickers = getTickers()
	//log.Println(t)

	wpUpdate := workerpool.New(768)
	for i, ticker := range tickers {
		ticker := ticker
		wpUpdate.Submit(func() {
			//fmt.Println(tickers)
			//ticker := tickers[i]

			log.Println("updating", i, ticker, date)
			var lines []line
			raws, err := db.Query(`select date,h,c from dailybars_duplicate where ticker=$1 and date>=$2 order by date asc limit 15`, ticker, date)
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
			// ReverseSlice(lines)
			updateChange(lines, db, ticker)

		})
	}
	wpUpdate.StopWait()
	return nil
}

func ReverseSlice(data interface{}) {
	value := reflect.ValueOf(data)
	if value.Kind() != reflect.Slice {
		panic(errors.New("data must be a slice type"))
	}
	valueLen := value.Len()
	for i := 0; i <= int((valueLen-1)/2); i++ {
		reverseIndex := valueLen - 1 - i
		tmp := value.Index(reverseIndex).Interface()
		value.Index(reverseIndex).Set(value.Index(i))
		value.Index(i).Set(reflect.ValueOf(tmp))
	}
}

var changesQry = map[int]string{
	// 0:  "UPDATE dailybars_duplicate SET change=$1 WHERE date=$2 AND ticker=$3",
	0:  "UPDATE dailybars_duplicate SET change1=$1 WHERE date=$2 AND ticker=$3",
	1:  "UPDATE dailybars_duplicate SET change2=$1 WHERE date=$2 AND ticker=$3",
	2:  "UPDATE dailybars_duplicate SET change3=$1 WHERE date=$2 AND ticker=$3",
	3:  "UPDATE dailybars_duplicate SET change4=$1 WHERE date=$2 AND ticker=$3",
	4:  "UPDATE dailybars_duplicate SET change5=$1 WHERE date=$2 AND ticker=$3",
	5:  "UPDATE dailybars_duplicate SET change6=$1 WHERE date=$2 AND ticker=$3",
	6:  "UPDATE dailybars_duplicate SET change7=$1 WHERE date=$2 AND ticker=$3",
	7:  "UPDATE dailybars_duplicate SET change8=$1 WHERE date=$2 AND ticker=$3",
	8:  "UPDATE dailybars_duplicate SET change9=$1 WHERE date=$2 AND ticker=$3",
	9:  "UPDATE dailybars_duplicate SET change10=$1 WHERE date=$2 AND ticker=$3",
	10: "UPDATE dailybars_duplicate SET change11=$1 WHERE date=$2 AND ticker=$3",
	11: "UPDATE dailybars_duplicate SET change12=$1 WHERE date=$2 AND ticker=$3",
	12: "UPDATE dailybars_duplicate SET change13=$1 WHERE date=$2 AND ticker=$3",
	13: "UPDATE dailybars_duplicate SET change14=$1 WHERE date=$2 AND ticker=$3",
}

func updateChange(lines []line, db DB, ticker string) {
	date := lines[0].date.Format("2006-01-02")
	first := lines[0]

	// fmt.Println(len(lines[1:]), ticker, date)
	for i, l := range lines[1:] {

		change := (l.high - first.close) / first.close * 100
		var qry = changesQry[i]

		_, err := db.Exec(qry, change, date, ticker)
		if err != nil {
			// continue errors.Wrap(err, "ERROR updatechange CANNOT UPDATE "+date+" "+ticker)
			log.Println("error can not update", err)
			_, err = db.Exec(qry, nil, date, ticker)
			if err != nil {
				// continue errors.Wrap(err, "ERROR updatechange CANNOT UPDATE "+date+" "+ticker)
				log.Fatalln("error can not update", err)
				return
			}
			return
		}
		// qry := fmt.Sprintf(changesQry[i])
		// fmt.Println(qry, change, ticker)

		// dbexec(qry, ticker, change, l, db)
	}

}

func conv2DecDigits(x float64) float64 {
	return math.Round(x*100) / 100
}
