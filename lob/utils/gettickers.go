package utils

import (
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

const URL_TICKERS = `http://oatsreportable.finra.org/OATSReportableSecurities-EOD.txt`

func getTickers() []string {
	res, err := http.Get(URL_TICKERS)
	if err != nil {
		log.Fatalln("cannot get data", err)
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

	return result
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

func (db *DB) GetTickersFromDailybars() ([]string, error) {
	var result []string
	raws, err := db.Query(`select distinct(ticker) from dailybars`)
	if err != nil {
		return nil, errors.Wrap(err, "ERROR SELECT updatechange")
	}
	_, _ = raws, result
	for raws.Next() {
		var ticker string
		err = raws.Scan(&ticker)
		if err != nil {
			log.Println(err, "ERROR SCAN updatechange")
			return nil, errors.Wrap(err, "ERROR SCAN updatechange")
		}
		result = append(result, ticker)
	}

	return result, nil
}
