package utils

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const URL_TICKERS = `http://oatsreportable.finra.org/OATSReportableSecurities-EOD.txt`
const URL_TICKER_DETAILS = `https://api.polygon.io/v1/meta/symbols/{}/company?apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

//const URL_TICKERS = `https://api.polygon.io/v2/reference/tickers?sort=ticker&perpage=50&page=%d&apiKey=wSriypADR8wfUaoyqqaZj_7pMDdRMp1p`

// func (db DB) updateTickers() error {

// }
func (db TransDB) InsertDataTableTransactions(ticker string, r *[]Result) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "Cannot begin transactions")
	}
	
	for _, data := range *r {
		timeName := time.Unix(data.T/1000, 0)
		log.Println("Create table ",timeName )
		
		if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS transactions_` + timeName + `;`); err != nil {
			log.Println("can not create table: ", err)
		}
		log.Println("Insert table ",timeName )

		qry := fmt.Sprintf(`INSERT INTO transactions_%s (date,ticker,t,q,i,c,p,s,e,x,r,z,time,transaction_type) 
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, timeName)
		_, err = tx.Exec(
			qry,
			time.Unix(data.T/1000, 0),
			ticker,
			data.T,
			data.Q,
			data.I,
			data.C,
			data.P,
			data.S,
			data.E,
			data.X,
			data.R,
			data.Z,
			time.Now().String(),
			1,
		)

		if err != nil {
			return errors.Wrap(err, "Cannot add query")
		} else {
			log.Println("inserted  data ", data.T/1000)
		}
		break
	}
	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "Cannot commit transaction")
	}
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
