package utils

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const URL_TICKERS = `http://oatsreportable.finra.org/OATSReportableSecurities-EOD.txt`
const URL_TICKER_DETAILS = `https://api.polygon.io/v1/meta/symbols/{}/company?apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

//const URL_TICKERS = `https://api.polygon.io/v2/reference/tickers?sort=ticker&perpage=50&page=%d&apiKey=wSriypADR8wfUaoyqqaZj_7pMDdRMp1p`

// func (db DB) updateTickers() error {

// }
func arrayToString(a []int, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
	//return strings.Trim(strings.Join(strings.Split(fmt.Sprint(a), " "), delim), "[]")
	//return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(a)), delim), "[]")
}

func (db TransDB) InsertDataTableTransactions(ticker string, r *[]Result) error {
	// tx, err := db.Begin()
	// if err != nil {
	// 	return errors.Wrap(err, "Cannot begin transactions")
	// }
	log.Println("insert run")
	dateInsert := (*r)[0].T
	timeName := time.Unix(0, dateInsert)
	month := int(timeName.Month())
	var monthString string
	if month < 10 {
		monthString = "0" + strconv.Itoa(month)
	} else {
		monthString = strconv.Itoa(month)
	}
	timeString := fmt.Sprintf("%d%s%s%s%d", timeName.Year(), "_", monthString, "_", timeName.Day())

	dropTable := fmt.Sprintf("%s%s", "DROP TABLE IF EXISTS transactions_", timeString)
	_, err := db.Exec(dropTable)
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
	// log.Println(queryStr)
	result, err := db.Exec(queryStr)
	if err != nil {
		log.Println("can not create table: ", err)
	}
	if result != nil {
		log.Println("creatae table ss ", result)
	}
	for _, data := range *r {
		// t = timeName.String()

		// var dt pgtype.Date
		timeInsert := fmt.Sprintf("%d%s%s%s%d", timeName.Year(), "-", monthString, "-", timeName.Day())
		qry := fmt.Sprintf(`INSERT INTO transactions_%s (date,ticker,t,q,i,c,p,s,e,x,r,z,time,transaction_type)
					VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, timeString)
		layout := "2006-01-02"
		t, err := time.Parse(layout, timeInsert)

		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(t)
		res, err := db.Exec(
			qry,
			t,
			ticker,
			data.T,
			data.Q,
			data.I,
			arrayToString(data.C, ","),
			data.P,
			data.S,
			data.E,
			data.X,
			data.R,
			data.Z,
			time.Now(),
			1,
		)
		log.Println("=====1", res)
		if err != nil {
			log.Println("can not insert data table: ", err, data.I)
			// log.Println(`data.Q,
			// data.I,
			// arrayToString(data.C, ","),
			// data.P,
			// data.S,
			// data.E,
			// data.X,
			// data.R,
			// data.Z,`,
			// 	data.Q,
			// 	data.I,
			// 	arrayToString(data.C, ","),
			// 	data.P,
			// 	data.S,
			// 	data.E,
			// 	data.X,
			// 	data.R,
			// 	data.Z)
			errors.Wrap(err, "Cannot add query")
		} else {
			log.Println("inserted  data ", data.T/1000)
		}
		// break
	}
	// err = db.Commit()
	// if err != nil {
	// 	return errors.Wrap(err, "Cannot commit transaction")
	// }
	return nil
}
func (db *DB) getTickers() ([]string, error) {
	res, err := http.Get(URL_TICKERS)
	if err != nil {
		return []string{}, errors.Wrap(err, "cannot get data")
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)

	if err != nil {
		log.Fatalln("cannot read body", err)
	}

	var result []string
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Split(line, `|`)
		if len(fields) < 3 {
			continue
		}
		if strings.Contains(fields[2], "NYSE") || strings.Contains(fields[2], "ARCA") || strings.Contains(fields[2], "NASDAQ") || strings.Contains(fields[2], "AMEX") {
			str := strings.Replace(fields[0], ` `, `.`, -1)

			result = append(result, str)
		}

	}

	return result, nil
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

type Ticker struct {
	Symbol   string         `db:"symbol"`
	ListDate sql.NullString `db:"listdate"`
}

func (db DB) GetTickersWithDateFromDB() ([]Ticker, error) {
	var res []Ticker
	rows, err := db.Queryx(`SELECT to_char(listdate, 'YYYY-MM-DD') as listdate,symbol FROM tickers where exchange in ('NYSE American', 'ARCA', 'NASDAQ', 'NASDAQ Capital Market', 'AMX', 'BATS', 'NYE')`)
	if err != nil {
		return []Ticker{}, errors.Wrap(err, "select symbol")
	}
	for rows.Next() {
		var ticker Ticker
		err = rows.StructScan(&ticker)
		if err != nil {
			return []Ticker{}, errors.Wrap(err, "select symbol scan")
		}
		res = append(res, ticker)
	}
	return res, err
}
