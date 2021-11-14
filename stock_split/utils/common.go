package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
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

const URL_TICKERS = `http://oatsreportable.finra.org/OATSReportableSecurities-EOD.txt`

func InitDB() (*DB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 user=postgres dbname=stockmarket password=P`AgD!9g!%~hz3M< sslmode=disable")
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}
	db.SetMaxOpenConns(150)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(60 * time.Minute)

	return &DB{db}, nil
}

func (db DB) GetTickersFromDB() ([]string, error) {
	var res []string
	rows, err := db.Query(`SELECT symbol FROM tickers where exchange in ('XASE', 'XNAS', 'EDGA', 'EDGX', 'XCHI', 'XNYS', 'ARCX', 'NXGS', 'IEXG', 'PHLX', 'BATY', 'BATS')`)
	if err != nil {
		return []string{}, errors.Wrap(err, "select symbol")
	}
	for rows.Next() {
		var str string
		err = rows.Scan(&str)
		if err != nil {
			return []string{}, errors.Wrap(err, "select symbol scan")
		}
		res = append(res, str)
	}
	return res, err
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
	}
	defer r.Body.Close()
	//fmt.Println("getJson", url, r.StatusCode)
	return json.NewDecoder(r.Body).Decode(target)
}

type OneMinResult struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

type OneMinTradesData struct {
	Results []OneMinResult `json:"results"`
}

type DailybarsResult struct {
	T       int64   `json:"t"`
	O       float64 `json:"o"`
	H       float64 `json:"h"`
	L       float64 `json:"l"`
	C       float64 `json:"c"`
	V       float64 `json:"v"`
	OneMinV float64
}

type DailybarsTradesData struct {
	Results []DailybarsResult `json:"results"`
}

const URL_BARS = `https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?unadjusted=false&sort=asc&limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`
const URL_BARS_1MIN = `https://api.polygon.io/v2/aggs/ticker/%s/range/1/minute/%s/%s?unadjusted=false&sort=asc&limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

func (d DB) GetDailybarsData(ticker string, start time.Time, end time.Time) ([]DailybarsResult, error) {
	log.Println("Getting", ticker)

	var res []DailybarsResult
	url := fmt.Sprintf(URL_BARS, ticker, start.Format("2006-01-02"), end.Format("2006-01-02"))
	td := DailybarsTradesData{}
	//startTime := time.Now()
	err := getJson(url, &td)
	if err != nil {
		return nil, errors.Wrap(err, "getData")
	}
	res = td.Results

	for i, r := range res {
		t := time.Unix(r.T/1000, 0)
		oneMinV, err := getData1Min(ticker, t)
		if err != nil {
			return nil, errors.Wrap(err, "error getting one minute data")
		}
		tmp := res[i]
		tmp.OneMinV = oneMinV
		res[i] = tmp
	}
	log.Println("Got", ticker)
	err = d.InsertDailybarsData(ticker, &res)
	if err != nil {
		return res, errors.Wrap(err, "insert failed")
	}
	return res, nil
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
	return max, nil
}

func (d DB) InsertDailybarsData(ticker string, r *[]DailybarsResult) error {
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

func conv2DecDigits(x float64) float64 {
	return math.Round(x*100) / 100
}

type line struct {
	date  time.Time
	high  float64
	close float64
}

func (db DB) updateChangeDuplicate(date string, tickers []string) error {
	//tickers = getTickers()
	//log.Println(t)

	wpUpdate := workerpool.New(120)
	for i, ticker := range tickers {
		ticker := ticker
		wpUpdate.Submit(func() {
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
			updateChanges(lines, db, ticker)

		})
	}
	wpUpdate.StopWait()
	return nil
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

func updateChanges(lines []line, db DB, ticker string) {
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
	}

}

func (db DB) updateChange(date string, tickers []string) error {
	//tickers = getTickers()
	//log.Println(t)

	for i, ticker := range tickers {
		//ticker := tickers[i]

		log.Println("updating", i, ticker, date)
		var lines []line
		raws, err := db.Query(`select date,h,c from dailybars where ticker=$1 and date<=$2 order by date desc limit 2`, ticker, date)
		if err != nil {
			return errors.Wrap(err, "ERROR SELECT updatechange")
		}
		_, _ = raws, lines
		for raws.Next() {
			var l line
			err = raws.Scan(&l.date, &l.high, &l.close)
			if err != nil {
				log.Println(err, "ERROR SCAN updatechange")
				return errors.Wrap(err, "ERROR SCAN updatechange")
			}
			lines = append(lines, l)
		}
		if len(lines) < 2 {
			log.Println("ERRORORORO", len(lines), ticker, date)
			continue
			//return errors.New("ERROR updatechange NOT ENOUGH RAWS " + ticker)
		}
		change := conv2DecDigits(lines[0].high-lines[1].close) / lines[1].close * 100
		_, err = db.Exec(`UPDATE dailybars SET change=$1 WHERE date=$2 AND ticker=$3`, change, lines[0].date.Format("2006-01-02"), ticker)
		if err != nil {
			return errors.Wrap(err, "ERROR updatechange CANNOT UPDATE "+lines[0].date.Format("2006-01-02")+" "+ticker)
		}

		c_c_change := conv2DecDigits(lines[0].close-lines[1].close) / lines[1].close * 100
		_, err = db.Exec(`UPDATE dailybars SET c_c_change=$1 WHERE date=$2 AND ticker=$3`, c_c_change, lines[0].date.Format("2006-01-02"), ticker)
		if err != nil {
			return errors.Wrap(err, "ERROR updatechange CANNOT UPDATE "+lines[0].date.Format("2006-01-02")+" "+ticker)
		}
	}
	return nil
}

type tickerData struct {
	Date      string  `db:"date"`
	High      float64 `db:"h"`
	Vol       int64   `db:"v"`
	Oneminvol int64   `db:"oneminvol"`
}

type resScan struct {
	Date      time.Time `db:"date"`
	Ticker    string    `db:"ticker"`
	H         float64   `db:"h"`
	V         int64     `db:"v"`
	Oneminvol int64     `db:"oneminvol"`
}

type res struct {
	Date      string  `db:"date"`
	Ticker    string  `db:"ticker"`
	H         float64 `db:"h"`
	V         int64   `db:"v"`
	Oneminvol int64   `db:"oneminvol"`
}

func (db DB) findBreakoutsUpdates(date, ticker string) {
	rows, err := db.Queryx("SELECT date,ticker,h,v,oneminvol FROM dailybars WHERE date=$1 and ticker=$2", date, ticker)
	if err != nil {
		log.Fatalln("CANNOT SELECT", err)
	}
	var bars []res
	for rows.Next() {
		var r resScan
		err := rows.StructScan(&r)
		if err != nil {
			log.Fatalln("CANNOT STRUCTSCAN", err)
		}
		bars = append(bars, res{
			Date:      r.Date.Format("2006-01-02"),
			Ticker:    r.Ticker,
			H:         r.H,
			V:         r.V,
			Oneminvol: r.Oneminvol,
		})
	}

	for _, b := range bars {
		db.findBreakoutsOneUpdates(b)
	}

}

func (db DB) findBreakoutsOneUpdates(bar res) {
	//log.Println("BAR=", bar)
	data := dailyBarsUpdates.m[bar.Ticker]
	//log.Println("DATA=", data)
	date := bar.Date
	var i int
	for i = 0; i < len(data)-1; i++ {
		if data[i].Date == date {
			break
		}
	}

	if i > 225 {
		// log.Println("DATA NOT FOUND", bar)
		return
	}

	var hBr, vBr, oneMBr int

	dateNow, err := time.Parse("2006-01-02", bar.Date)
	if err != nil {
		log.Println("Invalid date", bar, err)
	}
	before1Yr := dateNow.AddDate(-1, 0, 0)

	for x, d := range data[i:] {
		if d.Date == bar.Date {
			continue
		}
		barNow, err := time.Parse("2006-01-02", d.Date)
		if err != nil {
			log.Println("INVALID TIME", d)
			break
		}
		if barNow.Before(before1Yr) {
			break
		}
		if d.High >= bar.H {
			break
		}
		if x > 254 {
			break
		}
		hBr++
	}

	for x, d := range data[i:] {
		if d.Date == bar.Date {
			continue
		}
		barNow, err := time.Parse("2006-01-02", d.Date)
		if err != nil {
			log.Println("INVALID TIME", d)
			break
		}
		if barNow.Before(before1Yr) {
			break
		}
		if d.Vol >= bar.V {
			break
		}
		if x > 254 {
			break
		}
		vBr++
	}

	for x, d := range data[i:] {
		if d.Date == bar.Date {
			continue
		}
		barNow, err := time.Parse("2006-01-02", d.Date)
		if err != nil {
			log.Println("INVALID TIME", d)
			break
		}
		if barNow.Before(before1Yr) {
			break
		}
		if d.Oneminvol >= bar.Oneminvol {
			break
		}
		if x > 254 {
			break
		}
		oneMBr++
	}

	log.Println("Breakout found", bar.Ticker, bar.Date, hBr, vBr, oneMBr)
	_, err = db.Exec(`INSERT INTO breakout (date,ticker,h,v,oneminvol) 
				VALUES($1,$2,$3,$4,$5)`, bar.Date, bar.Ticker, hBr, vBr, oneMBr)
	if err != nil {
		log.Println("ERROR INSERTING", err)
	}
}

type loData struct {
	Date string `db:"date"`
	S    int64  `db:"s"`
}

// json fields in struct must be exported
type Result struct {
	// II int64   `json:"I,omitempy"`
	X int64   `json:"x"` // x
	P float64 `json:"p"` //  p*s
	//I string  `json:"i"`
	// E  int64   `json:"e"`
	// R  int64   `json:"r"`
	T int64 `json:"t"` //
	// Y  int64   `json:"y"`
	// F  int64   `json:"f"`
	// Q  int64   `json:"q"`
	C []int `json:"c"` // c
	S int64 `json:"s"` // s
	Z int64 `json:"z"`
	/*
		x integer,
		i bigint,
		z integer,
		p real,
		s bigint,
		c integer[],
		t bigint
	*/
}

type TradesData struct {
	Ticker       string   `json:"ticker"`
	ResultsCount int64    `json:"results_count"`
	DBLatency    int      `json:"db_latency"`
	Success      bool     `json:"success"`
	Results      []Result `json:"results"`
	//Map          map[string]interface{} `json:"map"`
}

const URL_TRADES = `https://api.polygon.io/v2/ticks/stocks/trades/%s/%s?limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`
const URL_TRADES_ADDITIONAL = `https://api.polygon.io/v2/ticks/stocks/trades/%s/%s?timestamp=%d&limit=50000&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

func (db DB) getTrades(ticker string, start time.Time) {
	var res []Result
	url := fmt.Sprintf(URL_TRADES, ticker, start.Format("2006-01-02"))
	td := TradesData{}
	//startTime := time.Now()
	err := getJson(url, &td)
	if err != nil {
		myClient = &http.Client{Timeout: 60 * time.Second}
		err = getJson(url, &td)
		if err != nil {
			log.Fatalln("cannot get json", err)
		}
	}
	log.Println("got", ticker, start, url)
	res = append(res, td.Results...)
	l := len(td.Results)
	//fmt.Println("got", len(d.Results))
	if len(td.Results) == 0 {
		//fmt.Println(ticker, start.Format("2006-01-02"), "total trades", len(res), "average=", 0, "time this run", time.Since(startTime))
		return
	}
	offset := td.Results[len(td.Results)-1].T

	for l == 50000 {
		td1, err := getMoreTrades(ticker, start, offset)
		if err != nil {
			log.Fatalln("!!!!!!!!!!!! cannot read body", err)
		}
		//fmt.Println("got", len(d1))
		res = append(res, td1...)
		if len(td1) == 0 {
			l = len(td1)
		} else {
			offset = td1[len(td1)-1].T
			l = len(td1)
		}

	}
	log.Println("got data", ticker, start)
	var largestOrder Result
	var sum int64
	var resFloat []float64
	var sumPrice float64
	for _, r := range res {
		sum += r.S
		sumPrice += r.P
		if r.S > largestOrder.S {
			largestOrder = r
		}
		resFloat = append(resFloat, float64(r.S))
	}
	count := len(res)
	average := float64(sum) / float64(len(res))
	averagePrice := sumPrice / float64(len(res))
	stddev := stat.StdDev(resFloat, nil)
	mean := stat.Mean(resFloat, nil)

	log.Println("Largest order, average", ticker, start, largestOrder)

	date := time.Unix(largestOrder.T/1000000000, 0).Format("2006-01-02")
	timeHuman := time.Unix(largestOrder.T/1000000000, 0)

	// _, err = tx.Exec(`INSERT INTO tradesraw (ev,sym,x,z,p,s,c,t) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
	// 	data.Ev, data.Ticker, data.X, data.Z, data.P, data.S, pq.Array(data.C), data.T)
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
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT ON CONSTRAINT largestorders_ticker_date_key
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
	} else {
		log.Println("INSERTED ", date, ticker, largestOrder.X, largestOrder.Z, largestOrder.P, largestOrder.S, largestOrder.C, largestOrder.T, timeHuman)
	}

	_, err = db.Exec(`INSERT INTO averages (date, ticker, avg, stddev, mean, count, avg_price) VALUES($1,$2,$3,$4,$5,$6,$7)`,
		date, ticker, average, stddev, mean, count, averagePrice)
	if err != nil {
		//	tx.Rollback()
		log.Println(err, fmt.Sprintln("ERROR CANT insert averages", date, ticker, average, stddev, mean, count))
	} else {
		log.Println("INSERTED AVERAGE ", date, ticker, average)
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

type resScanLo struct {
	Date   time.Time `db:"date"`
	Ticker string    `db:"ticker"`
	S      int64     `db:"s"`
}

type resLo struct {
	Date   string `db:"date"`
	Ticker string `db:"ticker"`
	S      int64  `db:"s"`
}

func (db DB) findLargestordersBreakoutsUpdates(date, ticker string) {
	rows, err := db.Queryx("SELECT date,ticker,s FROM largestorders WHERE date=$1 and ticker=$2", date, ticker)
	if err != nil {
		log.Fatalln("CANNOT SELECT", err)
	}
	var los []resLo
	for rows.Next() {
		var r resScanLo
		err := rows.StructScan(&r)
		if err != nil {
			log.Fatalln("CANNOT STRUCTSCAN", err)
		}
		los = append(los, resLo{
			Date:   r.Date.Format("2006-01-02"),
			Ticker: r.Ticker,
			S:      r.S,
		})
	}

	wpdates := workerpool.New(20)
	for _, lo := range los {
		lo := lo
		wpdates.Submit(func() {
			db.findLargestordersBreakoutsOneUpdates(lo)
		})

	}
	wpdates.StopWait()

}

func (db DB) findLargestordersBreakoutsOneUpdates(lo resLo) {
	//log.Println("BAR=", bar)
	data := largestOrdersUpdates.m[lo.Ticker]
	//log.Println("DATA=", data)
	date := lo.Date
	var i int
	for i = 0; i < len(data)-1; i++ {
		if data[i].Date == date {
			break
		}
	}

	if i > 225 {
		log.Println("DATA NOT FOUND", lo)
		return
	}

	var sBr int

	dateNow, err := time.Parse("2006-01-02", lo.Date)
	if err != nil {
		log.Println("Invalid date", lo, err)
	}
	before1Yr := dateNow.AddDate(-1, 0, 0)

	for x, d := range data[i:] {
		if d.Date == lo.Date {
			continue
		}
		barNow, err := time.Parse("2006-01-02", d.Date)
		if err != nil {
			log.Println("INVALID TIME", d)
			break
		}
		if barNow.Before(before1Yr) {
			break
		}
		if d.S >= lo.S {
			break
		}
		if x > 254 {
			break
		}
		sBr++
	}

	log.Println("Breakout found", lo.Ticker, lo.Date, sBr)

	_, err = db.Exec(`INSERT INTO largestorders_breakout (date,ticker,s) 
			VALUES($1,$2,$3)`, lo.Date, lo.Ticker, sBr)
	if err != nil {
		log.Println("ERROR INSERTING", err)
	}
}

func fetchRow(rows *sqlx.Rows) map[string]interface{} {
	var result = map[string]interface{}{}
	for rows.Next() {
		rows.MapScan(result)
	}

	return result
}

type loRecord struct {
	Date   string  `db:"date"`
	Ticker string  `db:"ticker"`
	S      int64   `db:"s"`
	P      float64 `db:"p"`
}

type averages struct {
	Ticker   string          `db:"ticker"`
	Avg      sql.NullFloat64 `db:"avg"`
	AvgPrice sql.NullFloat64 `db:"avg_price"`
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
	rows, err = db.Queryx("SELECT ticker,avg,avg_price FROM averages WHERE date=$1 and sym=$2;", start, ticker)
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
