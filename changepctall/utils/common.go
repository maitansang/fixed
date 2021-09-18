package utils

import (
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
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

func (db *DB) getTickers() ([]string, error) {
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

	// res, err := http.Get(URL_TICKERS)
	// if err != nil {
	// 	return []string{}, errors.Wrap(err, "cannot get data")
	// }
	// defer res.Body.Close()

	// body, err := ioutil.ReadAll(res.Body)

	// if err != nil {
	// 	log.Fatalln("cannot read body", err)
	// }

	// var result []string
	// for _, line := range strings.Split(string(body), "\n") {
	// 	fields := strings.Split(line, `|`)
	// 	if len(fields) < 3 {
	// 		continue
	// 	}
	// 	if strings.Contains(fields[2], "NYSE") || strings.Contains(fields[2], "ARCA") || strings.Contains(fields[2], "NASDAQ") || strings.Contains(fields[2], "AMEX") {
	// 		str := strings.Replace(fields[0], ` `, `.`, -1)

	// 		result = append(result, str)
	// 	}

	// }

	// return result, nil
}
