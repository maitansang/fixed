package utils

import (
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type DB struct {
	*gorm.DB
}

func InitDB() (*DB, error) {
	// handle db
	dsn := "host=52.116.150.66 user=postgres password=P`AgD!9g!%~hz3M< dbname=stockmarket port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Println("can not open db")
	}
	DB := &DB{
		db,
	}
	return DB, nil
}

type Ticker struct {
	ID     string `gorm:"primaryKey;autoIncrement:false"`
	Symbol string `json:"symbol" `
}
type Dailybar struct {
	ID     string `gorm:"primaryKey;autoIncrement:false"`
	Ticker string `json:"ticker" `
	V      string `json:"v"`
}
type Largestoder struct {
	ID     string `gorm:"primaryKey;autoIncrement:false"`
	Ticker string `json:"ticker" `
	// V      string `json:"v"`
}

type ShortInterest struct {
	ID     string `gorm:"primaryKey;autoIncrement:false"`
	Ticker string `json:"ticker" `
	// V      string `json:"v"`
}

func MainFunc() {
	db, err := InitDB()
	if err != nil {
		log.Println("can not init db", err)
	}

	tickers, err := condition1(db)
	if err != nil {
		log.Println("get condition1 error", err)
	}
	log.Println("=====tickers condition1=====", tickers, err)
}

// 1 Ticker name must be there in dailybars, largest_orders and short_interest and in each table it's row count must be 700
func condition1(db *DB) ([]Ticker, error) {
	log.Println("----begin condition1----")
	var tickers []Ticker

	var tickersDailybar []Dailybar
	var tickersLargestoder []Largestoder
	var tickersShortInterest []ShortInterest

	db.Distinct("symbol").Order("symbol desc").Find(&tickers)

	db.Raw(`SELECT *
		FROM dailybars 
		WHERE ticker IN (SELECT ticker
					   FROM dailybars
					   GROUP BY ticker HAVING COUNT(*) = 700)`).Scan(&tickersDailybar)
	db.Raw(`SELECT *
		FROM largest_orders 
		WHERE ticker IN (SELECT ticker
					   FROM largest_orders
					   GROUP BY ticker HAVING COUNT(*) = 700)`).Scan(&tickersLargestoder)
	db.Raw(`SELECT *
		FROM short_interest 
		WHERE ticker IN (SELECT ticker
					   FROM short_interest
					   GROUP BY ticker HAVING COUNT(*) = 700)`).Scan(&tickersShortInterest)
	
	for _, t := range tickers {
		check := false
		for _, td := range tickersDailybar {
			if t.Symbol == td.Ticker {
				check = true
			}
		}
		for _, tl := range tickersLargestoder {
			if t.Symbol == tl.Ticker {
				check = true
			}
		}
		for _, ts := range tickersShortInterest {
			if t.Symbol == ts.Ticker {
				check = true
			}
		}
		if !check {
			tickers = removeItem(tickers, t)
		}
	}

	return tickers, nil
}

func removeItem(tickers []Ticker, ticker Ticker)[]Ticker{
	for i, t := range tickers {
		if t.Symbol == ticker.Symbol {
			tickers = append(tickers[:i], tickers[i+1:]...)
		}
	}
	return tickers
}