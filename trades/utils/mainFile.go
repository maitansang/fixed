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
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"gonum.org/v1/gonum/stat"
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

	tickers, err := db.GetTickersFromDB()
	// tickers := []string{"AAPL"}
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

		createTransactionTable(transDB, timeString)
	}

	wp := workerpool.New(500)
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
	Ticker       string      `json:"ticker"`
	ResultsCount int64       `json:"results_count"`
	DBLatency    int         `json:"db_latency"`
	Success      bool        `json:"success"`
	Results      []Result `json:"results"`
	//Map          map[string]interface{} `json:"map"`
}

func (db DB) getTrades(ticker string, start time.Time, transDB *TransDB) {
	log.Println("============", ticker)
	var newRes []Result

	url := fmt.Sprintf(URL_TRADES, ticker, start.Format("2006-01-02"))
	newTd := TradesData{}

	//startTime := time.Now()
	err := getJson(url, &newTd)
	if err != nil {
		log.Fatalln("cannot get json 111111", err)
		myClient = &http.Client{Timeout: 60 * time.Second}
		err = getJson(url, &newTd)
		if err != nil {
			log.Fatalln("cannot get json", err)
		}
	}
	// log.Println("got", ticker, start, url)
	newRes = append(newRes, newTd.Results...)

	fmt.Println("----------2", len(newRes))

	l := len(newTd.Results)
	//fmt.Println("got", len(d.Results))
	if len(newTd.Results) == 0 {
		//fmt.Println(ticker, start.Format("2006-01-02"), "total trades", len(res), "average=", 0, "time this run", time.Since(startTime))
		return
	}
	offset := newTd.Results[len(newTd.Results)-1].T

	for l == 50000 {
		fmt.Println(ticker, "offset=", offset)
		td1, err := getMoreTrades(ticker, start, offset)
		if err != nil {
			log.Fatalln("!!!!!!!!!!!! cannot read body", err)
		}
		//fmt.Println("got", len(d1))
		newRes = append(newRes, td1...)
		if len(td1) == 0 {
			l = len(td1)
		} else {

			offset = td1[len(td1)-1].T
			l = len(td1)
		}
	}
	// log.Println("got data", ticker, start)

	// return
	if err := transDB.InsertDataTableTransactions(ticker, &newRes); err != nil {
		log.Println("Can not insert data table transaction")
	}

	var largestOrder Result
	var sum int64
	var sumPrice float64
	var resFloat []float64
	for _, r := range newRes {
		sum += r.S
		sumPrice += r.P
		if r.S > largestOrder.S {
			largestOrder = r
		}
		resFloat = append(resFloat, float64(r.S))
	}
	count := len(newRes)
	average := float64(sum) / float64(len(newRes))
	averagePrice := sumPrice / float64(len(newRes))
	stddev := stat.StdDev(resFloat, nil)
	mean := stat.Mean(resFloat, nil)

	// log.Println("Largest order, average", ticker, start, largestOrder)

	date := time.Unix(largestOrder.T/1000000000, 0).Format("2006-01-02")
	timeHuman := time.Unix(largestOrder.T/1000000000, 0)

	// _, err = tx.Exec(`INSERT INTO tradesraw (ev,ticker,x,z,p,s,c,t) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
	// 	data.Ev, data.Sym, data.X, data.Z, data.P, data.S, pq.Array(data.C), data.T)
	// tx, err := db.Beginx()
	// if err != nil {
	// 	log.Println(err, "Begin TX")
	// }
	_, err = db.Exec(`INSERT INTO largestorders (
			date,
			ticker,
			x,
			z,
			p,
			s,
			c,
			t,
			time
		)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT ON CONSTRAINT largestorders_sym_date_key
		DO
			UPDATE
			SET
				x=$3,
				z=$4,
				p=$5,
				s=$6,
				c=$7,
				t=$8,
				time=$9`,
		date, ticker, largestOrder.X, largestOrder.Z, largestOrder.P, largestOrder.S, pq.Array(largestOrder.C), largestOrder.T, timeHuman)

	if err != nil {
		//	tx.Rollback()
		log.Println(err, fmt.Sprintf("CANT UPSERT LARGE ORDER %d %s", largestOrder.T, ticker))
	}

	_, err = db.Exec(`INSERT INTO averages (date, ticker, avg, stddev, mean, count, avg_price) VALUES($1,$2,$3,$4,$5,$6,$7)`,
		date, ticker, average, stddev, mean, count, averagePrice)
	if err != nil {
		//	tx.Rollback()
		log.Println(err, fmt.Sprintln("ERROR CANT insert averages", date, ticker, average, stddev, mean, count))
	}

	db.persistTradeFeatures(db.extractTradesFeatures(ticker, newRes))
	// sort.Slice(res, func(i, j int) bool {
	// 	return res[i].S > res[j].S
	// })

	// top01 := int(len(res) / 1000)
	// if top01 < 2 {
	// 	top01 = 2
	// }
	// if top01 > len(res) {
	// 	top01 = len(res) - 1
	// }

	// tx, err := db.Begin()
	// if err != nil {
	// 	log.Println("Cannot creat TX", err)
	// }
	// for i := 0; i < top01; i++ {
	// 	//_, err = tx.Exec(`INSERT INTO tradesraw_top01 (`)
	// 	date := time.Unix(res[i].T/1000000000, 0).Format("2006-01-02")
	// 	timeHuman := time.Unix(res[i].T/1000000000, 0)

	// 	_, err = db.Exec(`INSERT INTO tradesraw_top01 (
	// 		date,
	// 		ticker,
	// 		x,
	// 		z,
	// 		p,
	// 		s,
	// 		c,
	// 		t,
	// 		time
	// 	)
	// 	VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
	// 		date, ticker, res[i].X, res[i].Z, res[i].P, res[i].S, pq.Array(res[i].C), res[i].T, timeHuman)
	// 	if err != nil {
	// 		log.Println("ERROR DB EXEC", err)
	// 	} else {
	// 		log.Println("INSERTED TOP01", date, ticker, i, top01)
	// 	}
	// }
	// err = tx.Commit()
	// if err != nil {
	// 	tx.Rollback()
	// 	log.Println("Can't commit, rollback")
	// }
	// log.Println("INSERTED 0.1% LARGES ORDERS", ticker, date)

	//}//
	//trades_categorization_date_ticker_symbol_trade_type_key

	//avg := calcAverage(&res)
	//fmt.Println(ticker, start.Format("2006-01-02"), "total trades", len(res), "time this run", time.Since(startTime))
	//startTime = time.Now()
	// cat := makeCategorization(ticker, start, &res)
	// //fmt.Println(ticker, start.Format("2006-01-02"), "Categorization", time.Since(startTime))
	// err = d.InsertCategorization(cat)
	// if err != nil {
	// 	log.Fatalln(err)
	// }
	//fmt.Println(ticker, start.Format("2006-01-02"), "Insert in SQL", time.Since(startTime))

}

func (db *DB) persistTradeFeatures(in []TradeFeatures) {
	for index := range in {
		tf := in[index]
		_, err := db.Exec(`INSERT INTO trades_features (
			"date",
			"ticker",
			"column",
			"count",
			"mean",
			"std",
			"min",
			"q1",
			"q2",
		 	"q3",
			"max",
		  	"uniq",
		 	"top",
		 	"freq"
		)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9,$10,$11,$12,$13,$14) ON CONFLICT ON CONSTRAINT trades_features_ticker_date_column_key
		DO
			UPDATE
			SET
				"date"=$1,
				"ticker"=$2,
				"column"=$3,
				"count"=$4,
				"mean"=$5,
				"std"=$6,
				"min"=$7,
				"q1"=$8,
				"q2"=$9,
				"q3"=$10,
				"max"=$11,
				"uniq"=$12,
				"top"=$13,
				"freq"=$14`,
			tf.Date, tf.Ticker, tf.Column, tf.Count, tf.Mean, tf.StdDev, tf.Min, tf.Q1, tf.Q2, tf.Q3, tf.Max, tf.Unique,
			tf.Top, tf.Freq)

		if err != nil {
			//	tx.Rollback()
			log.Println(err, fmt.Sprintf("CANT UPSERT Trade features %s %+v", tf.Date, tf))
		}
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
