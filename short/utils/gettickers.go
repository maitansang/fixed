package utils

import (
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
