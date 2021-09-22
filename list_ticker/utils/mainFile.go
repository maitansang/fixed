package utils

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/gammazero/workerpool"
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

func (db DB) getAllTicker() ([]string, error) {
	var tickers []string
	if err := db.DB.Table("tickers").
		Select("symbol").Scan(&tickers).Error; err != nil {
		log.Println("Error when get all ticker ", err)
		return nil, err
	}
	return tickers, nil
}

func MainFunc() {
	db, err := InitDB()
	if err != nil {
		log.Println("can not init db", err)
	}

	tickers, err := db.getAllTicker()
	if err != nil {
		log.Println("Error when get all ticker", err)
	}

	wpool := workerpool.New(100)
	for i, ticker := range tickers {
		index := i
		ticker := ticker
		wpool.Submit(func() {
			if !db.condition1(ticker) || !db.condition2(ticker) || !db.condition3(ticker) || !db.condition4(ticker) || !db.condition5(ticker) || !condition6(ticker) {
				removeItem(tickers, index)
			}
		})
	}
	wpool.StopWait()
	writeFile(tickers)
}

// 1 Ticker name must be there in dailybars, largest_orders and short_interest and in each table it's row count must be 700
func (db *DB) condition1(ticker string) bool {
	var count1, count2, count3 int64

	err := db.DB.Table("dailybars").
		Select("count(*)").
		Where("ticker = ?", ticker).
		Count(&count1).
		Error
	if err != nil {
		log.Fatalln("Error when find ticker has change greater than 700", err)
		return false
	}

	err = db.DB.Table("largestorders").
		Select("count(*)").
		Where("ticker = ?", ticker).
		Count(&count2).
		Error
	if err != nil {
		log.Fatalln("Error when find ticker has change greater than 700", err)
		return false
	}

	err = db.DB.Table("short_interest").
		Select("count(*)").
		Where("ticker = ?", ticker).
		Count(&count3).
		Error
	if err != nil {
		log.Fatalln("Error when find ticker has change greater than 700", err)
		return false
	}

	return count1 >= 700 && count2 >= 700 && count3 >= 700
}

// 2 Ticker must have lastest date closing price below 10
func (db *DB) condition2(ticker string) bool {
	var closingPrice float64
	err := db.DB.Raw("SELECT c FROM dailybars WHERE DATE=(SELECT MAX(DATE) FROM dailybars) AND ticker='AAPL'").Scan(&closingPrice).Error
	if err != nil {
		log.Println("Error when ticket has lastest date closing price below 10 ", err)
		return false
	}
	return closingPrice < 10
}

// 3 Ticker must have latest date volume greater than 50,000, volume is "v" in dailybars
func (db *DB) condition3(ticker string) bool {
	var count int64
	err := db.DB.Table("dailybars_duplicate").
		Select("count(*)").
		Where("v>5000 AND ticker = ?", ticker).Count(&count).Error
	if err != nil {
		log.Println("Error when count ticket has volume greater than 50000 ", err)
		return false
	}
	return count >= 1
}

// 4 Ticker must have at least 10 rows where it's change3 value is greater than 30 (dailybars_duplicate)
func (db *DB) condition4(ticker string) bool {
	var count int64
	err := db.DB.Table("dailybars_duplicate").
		Select("count(*)").
		Where("change3>30 AND ticker = ?", ticker).Count(&count).Error
	if err != nil {
		log.Println("Error ticker must have at least 10 rows where it's change3 value is greater than 30 ", err)
		return false
	}
	return count >= 10
}

// 5 Ticker must have atleast 100 rows where it's change value is either greater than 3 or below -3
func (db *DB) condition5(ticker string) bool {
	var count int64
	err := db.DB.Table("dailybars").
		Select("count(*)").
		Where("(change > 3 OR change < 3) AND ticker = ?", ticker).
		Count(&count).
		Error
	if err != nil {
		log.Println("Error when find ticker has change greater than 100", err)
		return false
	}
	return count >= 100
}

// 6 Ticker name must also be there in input_ticker.txt file
func condition6(ticker string) bool {
	file, err := os.Open("input_ticker.txt")
	if err != nil {
		log.Println("Error when get ticker from text file", err.Error())
		return false
	}
	defer file.Close()

	var inputTickers []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		inputTickers = append(inputTickers, scanner.Text())
	}
	for _, t := range inputTickers {
		if t == ticker {
			return true
		}
	}
	return false
}

func removeItem(tickers []string, i int) []string {
	copy(tickers[i:], tickers[i+1:])
	return tickers[:len(tickers)-1]
}

func writeFile(tickers []string) error {
	fmt.Println(tickers)
	file, err := os.Create("ticker.txt")
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, ticker := range tickers {
		fmt.Fprintln(w, ticker)
	}
	return w.Flush()
}
