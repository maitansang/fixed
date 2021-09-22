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

func MainFunc() {
	db, err := InitDB()
	if err != nil {
		log.Println("can not init db", err)
	}

	tickers, err := condition1(db)
	log.Println("=====", tickers, err)
}

// 1 Ticker name must be there in dailybars, largest_orders and short_interest and in each table it's row count must be 700
func condition1(db *DB) ([]Ticker, error) {
	log.Println("----")
	var tickers []Ticker
	db.Distinct("symbol").Order("symbol desc").Find(&tickers)
	return tickers, nil
}
