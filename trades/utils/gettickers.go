package utils

import (
	"database/sql"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

const URL_TICKERS = `http://oatsreportable.finra.org/OATSReportableSecurities-EOD.txt`
const URL_TICKER_DETAILS = `https://api.polygon.io/v1/meta/symbols/{}/company?apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

//const URL_TICKERS = `https://api.polygon.io/v2/reference/tickers?sort=ticker&perpage=50&page=%d&apiKey=wSriypADR8wfUaoyqqaZj_7pMDdRMp1p`

// func (db DB) updateTickers() error {

// }

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
