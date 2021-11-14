package utils

// import (
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"net/http"
// 	"os"
// 	"time"

// 	"github.com/gammazero/workerpool"
// 	"github.com/jmoiron/sqlx"
// 	"github.com/lib/pq"
// 	_ "github.com/lib/pq"
// 	"github.com/pkg/errors"
// )

// type DB struct {
// 	*sqlx.DB
// }

// func InitDB() (*DB, error) {
// 	db, err := sqlx.Open("postgres", "host=94.130.65.171 user=postgres dbname=stockmarket password=P`AgD!9g!%~hz3M< sslmode=disable")
// 	if err != nil {
// 		return nil, errors.Wrap(err, "connect to postgres:")
// 	}
// 	db.SetMaxOpenConns(150)
// 	db.SetMaxIdleConns(20)
// 	db.SetConnMaxLifetime(60 * time.Minute)

// 	// qry := `CREATE TABLE IF NOT EXISTS tradesraw_top01 (
// 	// 	id BIGSERIAL PRIMARY KEY,
// 	// 	date date,
// 	// 	ticker text,
// 	// 	x integer,
// 	// 	z integer,
// 	// 	p real,
// 	// 	s bigint,
// 	// 	c integer[],
// 	// 	t bigint,
// 	// 	time time,
// 	// 	UNIQUE(sym,t)
// 	// )`

// 	// _, err = db.Exec(qry)
// 	// if err != nil {
// 	// 	log.Fatalln("cannot create table tradesraw_top01", err)
// 	// }

// 	// for i := 0; i < 12; i++ {
// 	// 	qry := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS tradesraw_temp%d (
// 	// 		id BIGSERIAL PRIMARY KEY,
// 	// 		ev text,
// 	// 		ticker text,
// 	// 		x integer,
// 	// 		i bigint,
// 	// 		z integer,
// 	// 		p real,
// 	// 		s bigint,
// 	// 		c integer[],
// 	// 		t bigint
// 	// 	)`, i)
// 	// 	_, err = db.Exec(qry)
// 	// 	if err != nil {
// 	// 		log.Fatalln("cannot create able", i, err)
// 	// 	}
// 	// }
// 	d := &DB{
// 		db,
// 	}
// 	return d, nil
// }

// func MainFunc() {
// 	loc, err := time.LoadLocation("America/New_York")
// 	if err != nil {
// 		log.Fatalln("Can't set timezone", err)
// 	}
// 	time.Local = loc // -> this is setting the global timezone
// 	log.Println("time=", time.Now())

// 	db, err := InitDB()
// 	if err != nil {
// 		log.Fatalln("Can't open db", err)
// 	} else {
// 		log.Println("db connected ...")
// 	}
// 	defer db.Close()

// 	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS tradesraw (
// 		id BIGSERIAL PRIMARY KEY,
// 		date date,
// 		time time,
// 		ticker text,
// 		x integer,
// 		p real,
// 		i text,
// 		e bigint,
// 		r bigint,
// 		t bigint,
// 		y bigint,
// 		f bigint,
// 		q bigint,
// 		c integer[],
// 		s bigint,
// 		z integer
// 	)`)

// 	if err != nil {
// 		log.Fatalf(" error creating tradesraw %+v\n", err)
// 	}

// 	tickers, err := db.GetTickersWithDateFromDB()
// 	if err != nil {
// 		log.Fatalln("Can't get tickers", err)
// 	}

// 	//tickers := []string{"AAPL"}

// 	now := time.Now()

// 	// start, err := time.Parse("2006-01-02", os.Args[1])
// 	// if err != nil {
// 	// 	log.Fatalln("Can't parse time", err, os.Args[1], "Time must be in the format 2006-01-02")
// 	// }
// 	start, _ := time.Parse("2006-01-02", os.Args[2])
// 	globalEnd, _ := time.Parse("2006-01-02", os.Args[1])

// 	// globalEnd := start.AddDate(0, 0, -1)
// 	// end, _ := time.Parse("2006-01-02", "2019-01-01")
// 	wp := workerpool.New(100)
// 	for _, ticker := range tickers[:1] {
// 		tickerSUB := ticker.ticker // create copy of ticker
// 		tickerSUB = "CREX"

// 		var end time.Time
// 		if ticker.ListDate.Valid {
// 			listDate, err := time.Parse("2006-01-02", ticker.ListDate.String)
// 			if err != nil {
// 				log.Println("Error parsing date")
// 				end = globalEnd
// 			} else if listDate.Before(globalEnd) {
// 				end = globalEnd
// 			} else {
// 				end = listDate
// 			}
// 		} else {
// 			end = globalEnd
// 		}

// 		wp.Submit(func() {
// 			log.Println("START WORKER", tickerSUB)
// 			for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
// 				if t.Weekday() == 0 || t.Weekday() == 6 {
// 					continue
// 				}
// 				log.Println("GETTRADES", tickerSUB, t)
// 				db.getTrades(tickerSUB, t)
// 				// if err != nil {
// 				// 	log.Println("ERROR download data", err)
// 				// }

// 			}
// 			log.Println("END WORKER", tickerSUB)
// 		})
// 	}
// 	wp.StopWait()

// 	log.Println("Time taken for 1 month:", time.Since(now))
// }

// // const URL_TICKERS = `http://oatsreportable.finra.org/OATSReportableSecurities-EOD.txt`
// // const URL_TICKER_DETAILS = `https://api.polygon.io/v1/meta/symbols/{}/company?apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

// const URL_TRADES = `https://api.polygon.io/v2/ticks/stocks/trades/%s/%s?limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`
// const URL_TRADES_ADDITIONAL = `https://api.polygon.io/v2/ticks/stocks/trades/%s/%s?timestamp=%d&limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

// // json fields in struct must be exported
// type Result struct {
// 	// II int64   `json:"I,omitempy"`
// 	X int64   `json:"x"` // x
// 	P float64 `json:"p"` //  p*s
// 	I string  `json:"i"`
// 	E int64   `json:"e"`
// 	R int64   `json:"r"`
// 	T int64   `json:"t"` //
// 	Y int64   `json:"y"`
// 	F int64   `json:"f"`
// 	Q int64   `json:"q"`
// 	C []int   `json:"c"` // c
// 	S int64   `json:"s"` // s
// 	Z int64   `json:"z"`
// 	/*
// 		x integer,
// 		i bigint,
// 		z integer,
// 		p real,
// 		s bigint,
// 		c integer[],
// 		t bigint
// 	*/
// }

// type TradesData struct {
// 	Ticker       string   `json:"ticker"`
// 	ResultsCount int64    `json:"results_count"`
// 	DBLatency    int      `json:"db_latency"`
// 	Success      bool     `json:"success"`
// 	Results      []Result `json:"results"`
// 	//Map          map[string]interface{} `json:"map"`
// }

// func (db DB) getTrades(ticker string, start time.Time) {
// 	var res []Result
// 	url := fmt.Sprintf(URL_TRADES, ticker, start.Format("2006-01-02"))
// 	td := TradesData{}
// 	//startTime := time.Now()
// 	err := getJson(url, &td)
// 	if err != nil {
// 		myClient = &http.Client{Timeout: 60 * time.Second}
// 		err = getJson(url, &td)
// 		if err != nil {
// 			log.Fatalln("cannot get json", err)
// 		}
// 	}
// 	log.Println("got", ticker, start, url)
// 	res = append(res, td.Results...)
// 	l := len(td.Results)
// 	//fmt.Println("got", len(d.Results))
// 	if len(td.Results) == 0 {
// 		//fmt.Println(ticker, start.Format("2006-01-02"), "total trades", len(res), "average=", 0, "time this run", time.Since(startTime))
// 		return
// 	}
// 	offset := td.Results[len(td.Results)-1].T

// 	for l == 50000 {
// 		fmt.Println(ticker, "offset=", offset)
// 		td1, err := getMoreTrades(ticker, start, offset)
// 		if err != nil {
// 			log.Fatalln("!!!!!!!!!!!! cannot read body", err)
// 		}
// 		//fmt.Println("got", len(d1))
// 		res = append(res, td1...)
// 		if len(td1) == 0 {
// 			l = len(td1)
// 		} else {

// 			offset = td1[len(td1)-1].T
// 			l = len(td1)
// 		}
// 	}
// 	log.Println("got data", ticker, start)
// 	// var largestOrder Result
// 	// var sum int64
// 	// var sumPrice float64
// 	// var resFloat []float64

// 	wp := workerpool.New(192)
// 	for _, r := range res {
// 		r := r
// 		wp.Submit(func() {
// 			date := time.Unix(r.T/1000000000, 0)
// 			_, err = db.Exec(`INSERT INTO tradesraw (ticker,date,time,x,p,i,e,r,t,y,f,q,c,s,z) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
// 				ticker, date, date, r.X, r.P, r.I, r.E, r.R, r.T, r.Y, r.F, r.Q, pq.Array(r.C), r.S, r.Z)
// 			if err != nil {
// 				log.Printf("error\n%+v\n", err)
// 			}
// 			log.Println("INSERTED tradesraw", ticker, date)
// 		})

// 		// sum += r.S
// 		// sumPrice += r.P
// 		// if r.S > largestOrder.S {
// 		// 	largestOrder = r
// 		// }
// 		// resFloat = append(resFloat, float64(r.S))
// 	}
// 	wp.StopWait()
// 	// count := len(res)
// 	// average := float64(sum) / float64(len(res))
// 	// averagePrice := sumPrice / float64(len(res))
// 	// stddev := stat.StdDev(resFloat, nil)
// 	// mean := stat.Mean(resFloat, nil)

// 	// log.Println("Largest order, average", ticker, start, largestOrder)

// 	// date := time.Unix(largestOrder.T/1000000000, 0).Format("2006-01-02")
// 	// timeHuman := time.Unix(largestOrder.T/1000000000, 0)

// 	// _, err = tx.Exec(`INSERT INTO tradesraw (ev,ticker,x,z,p,s,c,t) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
// 	// 	data.Ev, data.Ticker, data.X, data.Z, data.P, data.S, pq.Array(data.C), data.T)
// 	// tx, err := db.Beginx()
// 	// if err != nil {
// 	// 	log.Println(err, "Begin TX")
// 	// }
// 	// _, err = db.Exec(`INSERT INTO largestorders (
// 	// 		date,
// 	// 		ticker,
// 	// 		x,
// 	// 		z,
// 	// 		p,
// 	// 		s,
// 	// 		c,
// 	// 		t,
// 	// 		time
// 	// 	)
// 	// 	VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT ON CONSTRAINT largestorders_ticker_date_key
// 	// 	DO
// 	// 		UPDATE
// 	// 		SET
// 	// 			x=$3,
// 	// 			z=$4,
// 	// 			p=$5,
// 	// 			s=$6,
// 	// 			c=$7,
// 	// 			t=$8,
// 	// 			time=$9`,
// 	// 	date, ticker, largestOrder.X, largestOrder.Z, largestOrder.P, largestOrder.S, pq.Array(largestOrder.C), largestOrder.T, timeHuman)

// 	// if err != nil {
// 	// 	//	tx.Rollback()
// 	// 	log.Println(err, fmt.Sprintf("CANT UPSERT LARGE ORDER %d %s", largestOrder.T, ticker))
// 	// } else {
// 	// 	log.Println("INSERTED ", date, ticker, largestOrder.X, largestOrder.Z, largestOrder.P, largestOrder.S, largestOrder.C, largestOrder.T, timeHuman)
// 	// }

// 	// _, err = db.Exec(`INSERT INTO averages (date, ticker, avg, stddev, mean, count, avg_price) VALUES($1,$2,$3,$4,$5,$6,$7)`,
// 	// 	date, ticker, average, stddev, mean, count, averagePrice)
// 	// if err != nil {
// 	// 	//	tx.Rollback()
// 	// 	log.Println(err, fmt.Sprintln("ERROR CANT insert averages", date, ticker, average, stddev, mean, count))
// 	// } else {
// 	// 	log.Println("INSERTED AVERAGE ", date, ticker, average)
// 	// }

// 	// sort.Slice(res, func(i, j int) bool {
// 	// 	return res[i].S > res[j].S
// 	// })

// 	// top01 := int(len(res) / 1000)
// 	// if top01 < 2 {
// 	// 	top01 = 2
// 	// }
// 	// if top01 > len(res) {
// 	// 	top01 = len(res) - 1
// 	// }

// 	// tx, err := db.Begin()
// 	// if err != nil {
// 	// 	log.Println("Cannot creat TX", err)
// 	// }
// 	// for i := 0; i < top01; i++ {
// 	// 	//_, err = tx.Exec(`INSERT INTO tradesraw_top01 (`)
// 	// 	date := time.Unix(res[i].T/1000000000, 0).Format("2006-01-02")
// 	// 	timeHuman := time.Unix(res[i].T/1000000000, 0)

// 	// 	_, err = db.Exec(`INSERT INTO tradesraw_top01 (
// 	// 		date,
// 	// 		ticker,
// 	// 		x,
// 	// 		z,
// 	// 		p,
// 	// 		s,
// 	// 		c,
// 	// 		t,
// 	// 		time
// 	// 	)
// 	// 	VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
// 	// 		date, ticker, res[i].X, res[i].Z, res[i].P, res[i].S, pq.Array(res[i].C), res[i].T, timeHuman)
// 	// 	if err != nil {
// 	// 		log.Println("ERROR DB EXEC", err)
// 	// 	} else {
// 	// 		log.Println("INSERTED TOP01", date, ticker, i, top01)
// 	// 	}
// 	// }
// 	// err = tx.Commit()
// 	// if err != nil {
// 	// 	tx.Rollback()
// 	// 	log.Println("Can't commit, rollback")
// 	// }
// 	// log.Println("INSERTED 0.1% LARGES ORDERS", ticker, date)

// 	//}//
// 	//trades_categorization_date_ticker_symbol_trade_type_key

// 	//avg := calcAverage(&res)
// 	//fmt.Println(ticker, start.Format("2006-01-02"), "total trades", len(res), "time this run", time.Since(startTime))
// 	//startTime = time.Now()
// 	// cat := makeCategorization(ticker, start, &res)
// 	// //fmt.Println(ticker, start.Format("2006-01-02"), "Categorization", time.Since(startTime))
// 	// err = d.InsertCategorization(cat)
// 	// if err != nil {
// 	// 	log.Fatalln(err)
// 	// }
// 	//fmt.Println(ticker, start.Format("2006-01-02"), "Insert in SQL", time.Since(startTime))

// }

// func getMoreTrades(ticker string, start time.Time, offset int64) ([]Result, error) {
// 	url := fmt.Sprintf(URL_TRADES_ADDITIONAL, ticker, start.Format("2006-01-02"), offset)
// 	d := TradesData{}
// 	err := getJson(url, &d)
// 	if err != nil {
// 		myClient = &http.Client{Timeout: 60 * time.Second}
// 		err = getJson(url, &d)
// 		if err != nil {
// 			return []Result{}, errors.Wrap(err, "cannot read body")
// 		}
// 	}
// 	return d.Results, nil
// }

// var myClient = &http.Client{Timeout: 60 * time.Second}

// func getJson(url string, target interface{}) error {
// 	var r *http.Response
// 	var err error
// 	r, err = myClient.Get(url)
// 	var i int64
// 	for ; err != nil; r, err = myClient.Get(url) { //|| r.StatusCode != 200
// 		time.Sleep(1 * time.Second)
// 		i++
// 		log.Println("ERROR GET JSON !!!!!!!!!!!!!!!! RETRYING ", i, err, url)
// 	}
// 	defer r.Body.Close()
// 	//fmt.Println("getJson", url, r.StatusCode)
// 	return json.NewDecoder(r.Body).Decode(target)
// }
