package utils

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/jmoiron/sqlx"
)

type loRecord struct {
	Date string  `db:"date"`
	Ticker  string  `db:"ticker"`
	S    int64   `db:"s"`
	P    float64 `db:"p"`
}

type averages struct {
	Ticker      string          `db:"ticker"`
	Avg      sql.NullFloat64 `db:"avg"`
	AvgPrice sql.NullFloat64 `db:"avg_price"`
}

func GetLos() {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalln("Can't set timezone", err)
	}
	time.Local = loc // -> this is setting the global timezone
	log.Println("time=", time.Now())

	// init db
	db, err := InitDB()
	if err != nil {
		log.Fatalln("Cannot init db", err)
	}
	db.SetMaxOpenConns(150)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(60 * time.Minute)

	// get tickers
	tickers, err := db.GetTickersFromDB()
	if err != nil {
		log.Fatalln("can not get data", err)
	}

	// start := time.Now() //.AddDate(0, 0, -3)
	start, _ := time.Parse("2006-01-02", os.Args[2])

	// end := start.AddDate(0, 0, -4)
	// end, _ := time.Parse("2006-01-02", "2019-01-01")
	end, _ := time.Parse("2006-01-02", os.Args[1])

	// for each ticker update
	wpavgPrice := workerpool.New(100)
	for _, ticker := range tickers {
		ticker := ticker
		wpavgPrice.Submit(func() {
			// get largestorder
			log.Println("START WORKER", ticker)
			for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
				if t.Weekday() == 0 || t.Weekday() == 6 {
					continue
				}
				log.Println("Update lo", ticker, t)
				UpdateLoValue(t, ticker, db)

			}
			log.Println("END WORKER", ticker)

		})
	}

	wpavgPrice.StopWait()

}

func fetchRow(rows *sqlx.Rows) map[string]interface{} {
	var result = map[string]interface{}{}
	for rows.Next() {
		rows.MapScan(result)
	}

	return result
}

func UpdateLoValue(start time.Time, ticker string, db *DB) {
	rows, err := db.Queryx("SELECT date,ticker,s,p FROM largestorders WHERE date=$1 and ticker=$2;", start, ticker)
	if err != nil {
		log.Fatalln("CANNOT SELECT", err)
	}

	var largestorder loRecord

	for rows.Next() {
		err := rows.StructScan(&largestorder)
		if err != nil {
			log.Fatalln("CANNOT STRUCTSCAN", err)
		}
	}

	if largestorder.Date == "" {
		log.Println("largest order nil", start, ticker)
		return
	}

	// get average
	rows, err = db.Queryx("SELECT ticker,avg,avg_price FROM averages WHERE date=$1 and ticker=$2;", start, ticker)
	if err != nil {
		log.Fatalln("CANNOT SELECT", err)
	}

	var avg averages

	for rows.Next() {
		err := rows.StructScan(&avg)
		if err != nil {
			log.Fatalln("CANNOT STRUCTSCAN", err)
		}
	}

	// get max
	maxRows, err := db.Queryx("select max(lo.s) from (select s,date from largestorders where date < $1 and ticker=$2 order by date desc limit 15) as lo;", start, ticker)
	maxS := fetchRow(maxRows)["max"]

	// get min
	minRows, err := db.Queryx("select min(lo.s) from (select s,date from largestorders where date < $1 and ticker=$2 order by date desc limit 15) as lo;", start, ticker)
	minS := fetchRow(minRows)["min"]

	if minS == nil {
		log.Println("result not found passing")
		return
	}
	if maxS == nil {
		log.Println("result not found passing")
		return
	}

	if minS.(int64) == 0 {
		log.Println("min is 0")
		return
	}

	days_ratio := (maxS.(int64) / minS.(int64))

	var avgOrderValue interface{}
	if avg.Avg.Valid && avg.AvgPrice.Valid {
		avgOrderValue = avg.Avg.Float64 * avg.AvgPrice.Float64
	} else {
		avgOrderValue = nil
	}

	qry := `INSERT INTO order_value (date,ticker,avg_order_value,lo_value,day_ratio_15)
					VALUES ($1,$2,$3,$4,$5)`
	_, err = db.Exec(
		qry,
		largestorder.Date,
		largestorder.Ticker,
		avgOrderValue,
		float64(largestorder.S)*largestorder.P,
		days_ratio,
	)
	if err != nil {
		log.Println("error inserting", err, largestorder)
	} else {
		log.Println("inserted  data ", largestorder)
	}
}
