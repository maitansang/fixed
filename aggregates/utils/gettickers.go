package utils

import (
	"github.com/pkg/errors"
)

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

func (db DB) CheckTickerFromDB(tickerInput string) (bool, error) {
	var ticker string
	// "SELECT userId, username, password FROM user WHERE username=?", userLogin.Username
	err := db.QueryRow("SELECT distinct symbol FROM tickers where symbol = $1 and exchange in ('XASE', 'XNAS', 'EDGA', 'EDGX', 'XCHI', 'XNYS', 'ARCX', 'NXGS', 'IEXG', 'PHLX', 'BATY', 'BATS')", tickerInput).Scan(&ticker)

	if err != nil {
		return false, errors.Wrap(err, "Not found ticker")
	}
	if ticker != "" {
		return true, nil
	}
	return false, nil
}

func (db DB) getTickers() ([]string, error) {
	var res []string
	rows, err := db.Query(`SELECT symbol FROM tickers`)
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

/*
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
	//var count int64
	var result []string
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Split(line, `|`)
		if len(fields) < 3 {
			continue
		}
		if strings.Contains(fields[2], "NYSE") || strings.Contains(fields[2], "ARCA") || strings.Contains(fields[2], "NASDAQ") || strings.Contains(fields[2], "AMEX") {
			str := strings.Replace(fields[0], ` `, `.`, -1)

			result = append(result, str)
			//count++
			//fmt.Println(">>", fields[0], fields[2])
		}

	}

	return result
}
*/
