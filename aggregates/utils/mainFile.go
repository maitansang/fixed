package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

const URL_TICKERS = `http://oatsreportable.finra.org/OATSReportableSecurities-EOD.txt`

// const URL_TICKER_DETAILS = `https://api.polygon.io/v1/meta/symbols/{}/company?apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`
//https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2020-01-14/2021-01-14?unadjusted=true&sort=asc&limit=120&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0
const URL_BARS = `https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?unadjusted=false&sort=asc&limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`
const URL_BARS_1MIN = `https://api.polygon.io/v2/aggs/ticker/%s/range/1/minute/%s/%s?unadjusted=false&sort=asc&limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

//const URL_BARS_1MIN_MORE = `https://api.polygon.io/v2/aggs/ticker/%s/range/1/minute/%s/%s?unadjusted=true&sort=asc&timestamp=%d&limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

// const URL_TRADE_TYPES_1 = `https://api.polygon.io/v1/meta/exchanges?apiKey=892GpHIePLd079gOmjAEfzOCbAyRv4dY`
// const URL_TRADE_TYPES_2 = `https://polygon.io/glossary/us/stocks/trade-conditions`
type DB struct {
	*sqlx.DB
}

func InitDB() (*DB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 user=postgres dbname=stockmarket password=P`AgD!9g!%~hz3M< sslmode=disable")
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}
	db.SetMaxOpenConns(150)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(60 * time.Minute)
	d := &DB{
		db,
		//&sync.Mutex{},
	}
	_, err = d.Exec(`CREATE TABLE IF NOT EXISTS dailybars (
    	id SERIAL PRIMARY KEY,
		date date,
		ticker text,
		o real,
		h real,
		l real,
		c real,
		v bigint,
		oneminvol bigint,
		UNIQUE(date,ticker)
		)`)
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}
	return d, nil
}

func MainFunc() {
	db, err := InitDB()
	if err != nil {
		log.Fatalln("Cannot init db", err)
	}
	wp := workerpool.New(20)
	res, err := db.GetTickersFromDB()
	// res := []string{"AAPL"}
	//start, end := time.Now().AddDate(-1, 0, 0), time.Now()
	// start := time.Now().AddDate(0, 0, -8)
	// end := time.Now().AddDate(0, 0, -1)

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalln("Can't set timezone", err)
	}
	time.Local = loc // -> this is setting the global timezone
	log.Println("time=", time.Now())

	scriptStart := time.Now()

	// start := time.Now().AddDate(0, 0, -4)
	// start, _ := time.Parse("2006-01-02", "2019-01-01")
	// start, _ := time.Parse("2006-01-02", "2021-04-21")
	// end, _ := time.Parse("2006-01-02", "2021-05-09")

	start, _ := time.Parse("2006-01-02", os.Args[1])
	end, _ := time.Parse("2006-01-02", os.Args[2])
	// end := time.Now()
	log.Println("start: ", start, "end: ", end)
	for _, st := range res {
		stock := st
		wp.Submit(func() {
			fmt.Println("getting for", stock)
			err := db.getData(stock, start.AddDate(0,0,+1), end)
			if err != nil {
				log.Println("ERROR", stock, err)
			}
		})
	}
	wp.StopWait()
	log.Println("Time to load dailybars", time.Since(scriptStart))
}

func (d DB) getData(ticker string, start time.Time, end time.Time) error {
	log.Println("Getting", ticker)
	var res []Result
	url := fmt.Sprintf(URL_BARS, ticker, start.Format("2006-01-02"), end.Format("2006-01-02"))
	td := TradesData{}
	//startTime := time.Now()
	err := getJson(url, &td)
	if err != nil {
		return errors.Wrap(err, "getData")
	}
	res = td.Results
	fmt.Println("=====",ticker, res) // daily bars in res

	for i, r := range res {
		t := time.Unix(r.T/1000, 0)
		oneMinV, err := getData1Min(ticker, t)
		if err != nil {
			return errors.Wrap(err, "error getting one minute data")
		}
		tmp := res[i]
		tmp.OneMinV = oneMinV
		res[i] = tmp
	}
	//fmt.Println(ticker, res)
	log.Println("Got", ticker)
	err = d.InsertData(ticker, &res)
	if err != nil {
		return errors.Wrap(err, "insert failed")
	}
	return nil
}

func (d DB) InsertData(ticker string, r *[]Result) error {
	tx, err := d.Begin()
	if err != nil {
		return errors.Wrap(err, "Cannot begin transaction")
	}
	for _, data := range *r {
		qry := `INSERT INTO dailybars (date,ticker,o,h,l,c,v,oneminvol) 
					VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`
		v := int64(data.V)
		oneminv := int64(data.OneMinV)
		_, err = tx.Exec(qry, time.Unix(data.T/1000, 0), ticker, conv2DecDigits(data.O),
			conv2DecDigits(data.H), conv2DecDigits(data.L), conv2DecDigits(data.C), v,
			oneminv)
		if err != nil {
			return errors.Wrap(err, "Cannot add query")
		} else {
			log.Println("inserted  data ", data.T/1000)
		}
	}
	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "Cannot commit transaction")
	}
	return nil
}

func getData1Min(ticker string, start time.Time) (float64, error) {
	var res1m []OneMinResult
	url1m := fmt.Sprintf(URL_BARS_1MIN, ticker, start.Format("2006-01-02"), start.Format("2006-01-02"))
	tdmin := OneMinTradesData{}
	err := getJson(url1m, &tdmin)
	if err != nil {
		return 0.0, errors.Wrap(err, "GETJSON")
	}
	res1m = tdmin.Results
	var max float64
	for _, r := range res1m {
		if r.V > max {
			max = r.V
		}
	}
	//fmt.Println(start.Format("2006-01-02"), max)
	return max, nil
}

// json fields in struct must be exported
type OneMinResult struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

type OneMinTradesData struct {
	Results []OneMinResult `json:"results"`
}

type Result struct {
	T       int64   `json:"t"`
	O       float64 `json:"o"`
	H       float64 `json:"h"`
	L       float64 `json:"l"`
	C       float64 `json:"c"`
	V       float64 `json:"v"`
	OneMinV float64
}

type TradesData struct {
	Results []Result `json:"results"`
}

func conv2DecDigits(x float64) float64 {
	return math.Round(x*100) / 100
}

// 16:16

// func (d DB) getTrades(ticker string, start time.Time, end time.Time) {
// 	var res []Result
// 	url := fmt.Sprintf(URL_TRADES, ticker, start.Format("2006-01-02"), end.Format("2006-01-02"))
// 	td := TradesData{}
// 	//startTime := time.Now()
// 	err := getJson(url, &td)
// 	if err != nil {
// 		log.Fatalln("cannot get json", err)
// 	}
// 	res = append(res, td.Results...)
// 	l := len(td.Results)
// 	//fmt.Println("got", len(d.Results))
// 	if len(td.Results) == 0 {
// 		//fmt.Println(ticker, start.Format("2006-01-02"), "total trades", len(res), "average=", 0, "time this run", time.Since(startTime))
// 		return
// 	}
// 	offset := td.Results[len(td.Results)-1].T
// 	for l == 50000 {
// 		//fmt.Println("offset=", offset)
// 		td1, err := getMoreTrades(ticker, start, offset)
// 		if err != nil {
// 			log.Fatalln("cannot read body", err)
// 		}
// 		//fmt.Println("got", len(d1))
// 		res = append(res, td1...)
// 		offset = td1[len(td1)-1].T
// 		l = len(td1)
// 	}
// 	//avg := calcAverage(&res)
// 	//fmt.Println(ticker, start.Format("2006-01-02"), "total trades", len(res), "time this run", time.Since(startTime))
// 	//startTime = time.Now()
// 	cat := makeCategorization(ticker, start, &res)
// 	//fmt.Println(ticker, start.Format("2006-01-02"), "Categorization", time.Since(startTime))
// 	err = d.InsertCategorization(cat)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	//fmt.Println(ticker, start.Format("2006-01-02"), "Insert in SQL", time.Since(startTime))

// }

// func (d DB) InsertCategorization(c []Categorization) error {
// 	tx, err := d.Begin()
// 	if err != nil {
// 		return errors.Wrap(err, "Cannot begin transaction")
// 	}
// 	for _, data := range c {
// 		qry := `INSERT INTO trades_categorization (date,ticker_symbol,trade_type,total_trade_value,trade_count)
// 					VALUES ($1,$2,$3,$4,$5)`
// 		_, err = tx.Exec(qry, data.date, data.ticker, data.tradeType, conv2DecDigits(data.totalTradeValue), data.tradeCount)
// 		if err != nil {
// 			return errors.Wrap(err, "Cannot add query")
// 		}
// 	}
// 	err = tx.Commit()
// 	if err != nil {
// 		return errors.Wrap(err, "Cannot commit transaction")
// 	}
// 	return nil
// }

// func getMoreTrades(ticker string, start time.Time, offset int64) ([]Result, error) {
// 	url := fmt.Sprintf(URL_TRADES_ADDITIONAL, ticker, start.Format("2006-01-02"), offset)
// 	d := TradesData{}
// 	err := getJson(url, &d)
// 	if err != nil {
// 		return []Result{}, errors.Wrap(err, "cannot read body")
// 	}
// 	return d.Results, nil
// }

var myClient = &http.Client{Timeout: 60 * time.Second}

func getJson(url string, target interface{}) error {
	var r *http.Response
	var err error
	r, err = myClient.Get(url)
	var i int64
	for ; err != nil; r, err = myClient.Get(url) { //|| r.StatusCode != 200
		time.Sleep(1 * time.Second)
		i++
		fmt.Println("!!!!!!!!!!!!!!!! RETRYING ", i, err, url)
	}
	defer r.Body.Close()
	//fmt.Println("getJson", url, r.StatusCode)
	return json.NewDecoder(r.Body).Decode(target)
}
