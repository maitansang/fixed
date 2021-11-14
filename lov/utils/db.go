package utils

import (
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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

type DB struct {
	*sqlx.DB
}

func InitDB() (*DB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 user=postgres dbname=stockmarket password=P`AgD!9g!%~hz3M< sslmode=disable")
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}
	db.SetMaxOpenConns(150)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(60 * time.Minute)
	d := &DB{
		db,
		//&sync.Mutex{},
	}
	_, err = d.Exec(`CREATE TABLE IF NOT EXISTS dailybars (
    	id SERIAL PRIMARY KEY,
		date date,
		ticker text,
		o real,
		h real,
		l real,
		c real,
		v bigint,
		oneminvol bigint,
		UNIQUE(date,ticker)
		)`)
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}

	_, err = d.Exec(`CREATE TABLE IF NOT EXISTS tickers(
		id SERIAL PRIMARY KEY,
		symbol text,
		name text,
		exchange text,
		marketcap bigint,
		sector text,
		industry text,
		tags text [],
		url text,
		UNIQUE(symbol,exchange)
	)`)

	if err != nil {
		return nil, errors.Wrap(err, "create table:")
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS order_value (
    	id SERIAL PRIMARY KEY,
		date date,
		ticker text,
		avg_order_value numeric,
		lo_value numeric,
		day_ratio_15 numeric,
		UNIQUE(date,ticker)
		)`)

	if err != nil {
		log.Fatalln("Error creating order_value table", err)
	}

		return d, nil
}
