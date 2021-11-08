package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type DB struct {
	*sqlx.DB
}
type TransDB struct {
	*sqlx.DB
}

func InitDB() (*DB, *TransDB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 user=postgres dbname=stockmarket password=P`AgD!9g!%~hz3M< sslmode=disable")
	if err != nil {
		return nil, nil, errors.Wrap(err, "connect to postgres:")
	}
	db.SetMaxOpenConns(150)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(60 * time.Minute)

	tdb, err := sqlx.Open("postgres", "host=52.116.150.66 port=5433 user=dev_user dbname=transaction_db password=Dev$54321")
	if err != nil {
		return nil, nil, errors.Wrap(err, "connect to postgres:")
	}
	db.SetMaxOpenConns(500)
	db.SetMaxIdleConns(20000)
	db.SetConnMaxLifetime(60 * time.Minute)

	d := &DB{
		db,
	}
	transDB := &TransDB{
		tdb,
	}
	return d, transDB, nil
}

func createTransactionTable(transDB *TransDB, timeString string) error {
	dropTable := fmt.Sprintf("%s%s", "DROP TABLE IF EXISTS transactions_", timeString)
	_, err := transDB.Exec(dropTable)
	if err != nil {
		log.Println("can not drop table: ", err)
	}
	queryStr := fmt.Sprintf("%s%s%s", "CREATE TABLE IF NOT EXISTS transactions_", timeString, `(
		date date,
		ticker text,
		t bigint,
		q integer,
		i bigint,
		c text,
		p numeric,
		s numeric,
		e integer,
		x integer,
		r integer,
		z integer,
		time time without time zone,
		transaction_type integer
		)`)
	result, err := transDB.Exec(queryStr)
	if err != nil {
		return err
	}
	if result != nil {
		return err
	}

	return nil
}

func MainFunc() {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalln("Can't set timezone", err)
	}
	time.Local = loc // -> this is setting the global timezone
	log.Println("time=", time.Now())

	db, transDB, err := InitDB()
	log.Println(transDB.Ping())
	if err != nil {
		log.Fatalln("Can't open db", err)
	} else {
		log.Println("db connected ...")
	}
	defer db.Close()
	defer transDB.Close()

	// tickers, err := db.GetTickersFromDB()
	tickers := []string{"AAPL"}
	if err != nil {
		log.Fatalln("Can't get tickers", err)
	}

	start, _ := time.Parse("2006-01-02", os.Args[2])
	end, _ := time.Parse("2006-01-02", os.Args[1])

	for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
		if t.Weekday() == 0 || t.Weekday() == 6 {
			continue
		}

		timeString := t.Format("2006-01-02")
		timeString = strings.Replace(timeString, "-", "_", 2)

		err := createTransactionTable(transDB, timeString)
		if err != nil {
			log.Fatalln("Can't create table", err)
		}
	}

	wp := workerpool.New(200)
	for _, ticker := range tickers {
		tickerSUB := ticker // create copy of ticker
		wp.Submit(func() {
			for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
				if t.Weekday() == 0 || t.Weekday() == 6 {
					continue
				}
				db.getTrades(tickerSUB, t, transDB)
			}
		})
	}
	wp.StopWait()
}

// const URL_TICKERS = `http://oatsreportable.finra.org/OATSReportableSecurities-EOD.txt`
// const URL_TICKER_DETAILS = `https://api.polygon.io/v1/meta/symbols/{}/company?apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

const URL_TRADES = `https://api.polygon.io/v2/ticks/stocks/trades/%s/%s?limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`
const URL_TRADES_ADDITIONAL = `https://api.polygon.io/v2/ticks/stocks/trades/%s/%s?timestamp=%d&limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

// json fields in struct must be exported
type Result struct {
	X int64   `json:"x"` // x
	P float64 `json:"p"` //  p*s
	I string  `json:"i"`
	E int64   `json:"e"`
	R int64   `json:"r"`
	T int64   `json:"t"` //
	// 	Y int64   `json:"y"`
	// 	F int64   `json:"f"`
	Q int64 `json:"q"`
	C []int `json:"c"` // c
	S int64 `json:"s"` // s
	Z int64 `json:"z"`
}
type TradesData struct {
	Ticker       string   `json:"ticker"`
	ResultsCount int64    `json:"results_count"`
	DBLatency    int      `json:"db_latency"`
	Success      bool     `json:"success"`
	Results      []Result `json:"results"`
	//Map          map[string]interface{} `json:"map"`
}

func (db DB) getTrades(ticker string, start time.Time, transDB *TransDB) {
	log.Println("============", ticker)
	var newRes []Result

	url := fmt.Sprintf(URL_TRADES, ticker, start.Format("2006-01-02"))
	newTd := TradesData{}

	err := getJson(url, &newTd)
	if err != nil {
		log.Fatalln("cannot get json ", err)
		myClient = &http.Client{Timeout: 60 * time.Second}
		err = getJson(url, &newTd)
		if err != nil {
			log.Fatalln("cannot get json", err)
		}
	}
	newRes = append(newRes, newTd.Results...)

	l := len(newTd.Results)
	if len(newTd.Results) == 0 {
		return
	}
	offset := newTd.Results[len(newTd.Results)-1].T
	for l == 50000 {
		fmt.Println(ticker, "offset=", offset)
		td1, err := getMoreTrades(ticker, start, offset)
		if err != nil {
			log.Fatalln("!!!!!!!!!!!! cannot read body", err)
		}
		newRes = append(newRes, td1...)
		if len(td1) == 0 {
			l = len(td1)
		} else {

			offset = td1[len(td1)-1].T
			l = len(td1)
		}
	}
	if err := transDB.InsertDataTableTransactions(ticker, &newRes); err != nil {
		log.Println("Can not insert data table transaction")
	}
}

func getMoreTrades(ticker string, start time.Time, offset int64) ([]Result, error) {
	url := fmt.Sprintf(URL_TRADES_ADDITIONAL, ticker, start.Format("2006-01-02"), offset)
	d := TradesData{}
	err := getJson(url, &d)
	if err != nil {
		myClient = &http.Client{Timeout: 60 * time.Second}
		err = getJson(url, &d)
		if err != nil {
			return []Result{}, errors.Wrap(err, "cannot read body")
		}
	}
	return d.Results, nil
}

var myClient = &http.Client{Timeout: 60 * time.Second}

func getJson(url string, target interface{}) error {
	var r *http.Response
	var err error
	r, err = myClient.Get(url)
	var i int64
	for ; err != nil; r, err = myClient.Get(url) { //|| r.StatusCode != 200
		time.Sleep(1 * time.Second)
		i++
		log.Println("ERROR GET JSON !!!!!!!!!!!!!!!! RETRYING ", i, err, url)
	}
	defer r.Body.Close()
	//fmt.Println("getJson", url, r.StatusCode)
	return json.NewDecoder(r.Body).Decode(target)
}
