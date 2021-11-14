package utils

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type DB struct {
	*gorm.DB
}

var conditions []string

func InitDB() (*DB, error) {
	// handle db
	dsn := "host=52.116.150.66 user=postgres password=P`AgD!9g!%~hz3M< dbname=stockmarket port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Println("can not open db")
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Println("Error when init sql db")
	}
	sqlDB.SetMaxOpenConns(150)
	sqlDB.SetMaxIdleConns(20)
	sqlDB.SetConnMaxLifetime(60 * time.Minute)
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

	sqlDB, err := db.DB.DB()
	if err != nil {
		log.Println("Error when init sql db")
	}
	defer sqlDB.Close()

	if len(os.Args) < 2 {
		log.Fatalln("Please provide conditions")
	}

	conditionString := os.Args[1]
	conditions = strings.Split(conditionString, ",")
	var tickerResource []string
	fmt.Println(!isValid("6"))
	// allTickers, err := db.getAllTicker()
	if err != nil {
		log.Println("Error when get all ticker", err)
	}

	tickersCondition1, err := db.condition1()
	if err != nil {
		log.Fatal("Err condition 1 ", err)
	}
	fmt.Println("len 1", len(tickersCondition1))

	tickersCondition2, err := db.condition2()
	if err != nil {
		log.Fatal("Err condition 2 ", err)
	}
	fmt.Println("len 2", len(tickersCondition2))

	tickersCondition3, err := db.condition3()
	if err != nil {
		log.Fatal("Err condition 3 ", err)
	}
	fmt.Println("len 3", len(tickersCondition3))

	tickersCondition4, err := db.condition4()
	if err != nil {
		log.Fatal("Err condition 4 ", err)
	}
	fmt.Println("len 4", len(tickersCondition4))

	tickersCondition5, err := db.condition5()
	if err != nil {
		log.Fatal("Err condition 5 ", err)
	}
	fmt.Println("len 5", len(tickersCondition5))

	tickersCondition6, err := db.condition6()
	if err != nil {
		log.Fatal("Err condition 6 ", err)
	}
	fmt.Println("len 6", len(tickersCondition6))
	switch conditions[0] {
	case "1":
		tickerResource = tickersCondition1
	case "2":
		tickerResource = tickersCondition2
	case "3":
		tickerResource = tickersCondition3
	case "4":
		tickerResource = tickersCondition4
	case "5":
		tickerResource = tickersCondition5
	case "6":
		tickerResource = tickersCondition6
	}
	mapTickerCondition := make(map[string][]string)
	for _, v := range conditions {
		switch v {
		case "1":
			mapTickerCondition[v] = tickersCondition1
		case "2":
			mapTickerCondition[v] = tickersCondition2
		case "3":
			mapTickerCondition[v] = tickersCondition3
		case "4":
			mapTickerCondition[v] = tickersCondition4
		case "5":
			mapTickerCondition[v] = tickersCondition5
		case "6":
			mapTickerCondition[v] = tickersCondition6
		}
	}
	var resultTickers []string
	for _, item := range tickerResource {
		if containsMap(mapTickerCondition, item) {
			resultTickers = append(resultTickers, item)
		}
	}
	writeFile(resultTickers)
	fmt.Println("Result", resultTickers)
	fmt.Println("Len result", len(resultTickers))

}
func containsMap(mapTickerCondition map[string][]string, str string) bool {
	var status bool
	for _, v := range mapTickerCondition {
		if contains(v, str) {
			status = true
		} else {
			status = false
			break
		}
	}
	return status
}
func writeFile(tickers []string) error {
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

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}

func isValid(condition string) bool {
	for _, c := range conditions {
		if c == condition {
			return true
		}
	}
	return false
}

// 1 Ticker name must be there in dailybars, largest_orders and short_interest and in each table it's row count must be 700
func (db *DB) condition1() ([]string, error) {
	var newTickers []string
	if !isValid("1") {
		return newTickers, nil
	}
	var newTickersDailybars []string
	err := db.DB.Raw("select ticker from dailybars where ticker is not null group by ticker having count(id) > 700").Scan(&newTickersDailybars).Error
	if err != nil {
		log.Fatal("Error when find ticker has change greater than 700", err)
		return newTickers, err
	}

	var newTickersLargestorders []string
	err = db.DB.Raw("select ticker from largestorders where ticker is not null group by ticker having count(id) > 700").Scan(&newTickersLargestorders).Error
	if err != nil {
		log.Fatal("Error when find ticker has change greater than 700", err)
		return newTickers, err
	}

	var newTickersShortInterest []string
	err = db.DB.Raw("select ticker  from short_interest where ticker is not null group by ticker having count(id) > 700").Scan(&newTickersShortInterest).Error
	if err != nil {
		log.Fatal("Error when find ticker has change greater than 700", err)
		return newTickers, err
	}
	for _, v := range newTickersDailybars {
		if contains(newTickersLargestorders, v) && contains(newTickersShortInterest, v) {
			newTickers = append(newTickers, v)
		}
	}
	return newTickers, nil
}

// 2 Ticker must have lastest date closing price below 10
func (db *DB) condition2() ([]string, error) {
	var tickers []string

	if !isValid("2") {
		return tickers, nil
	}
	err := db.DB.Raw("SELECT ticker FROM dailybars WHERE DATE=(SELECT MAX(DATE) FROM dailybars) AND c < 10").Scan(&tickers).Error
	if err != nil {
		log.Println("Error when ticket has lastest date closing price below 10 ", err)
		return tickers, err
	}
	return tickers, err
}

// 3 Ticker must have latest date volume greater than 50,000, volume is "v" in dailybars
func (db *DB) condition3() ([]string, error) {
	var tickers []string

	if !isValid("3") {
		return tickers, nil
	}

	var maxDate string

	err := db.DB.Table("dailybars").
		Select("max(date)").Scan(&maxDate).Error
	if err != nil {
		log.Println("Error when count ticket has volume greater than 50000 ", err)
		return tickers, err
	}

	err = db.DB.Table("dailybars").
		Select("ticker").
		Where("v>5000 AND date= ?", maxDate).Scan(&tickers).Error
	if err != nil {
		log.Println("Error when count ticket has volume greater than 50000 ", err)
		return tickers, err
	}

	return tickers, err
}

// 4 Ticker must have at least 10 rows where it's change3 value is greater than 30 (dailybars_duplicate)
func (db *DB) condition4() ([]string, error) {
	var tickers []string
	if !isValid("4") {
		return tickers, nil
	}

	err := db.DB.Raw("Select ticker from dailybars_duplicate where change3>30 group by ticker having count(ticker)>=10").Scan(&tickers).Error
	if err != nil {
		log.Println("Error ticker must have at least 10 rows where it's change3 value is greater than 30 ", err)
		return tickers, err
	}
	return tickers, err
}

// 5 Ticker must have atleast 100 rows where it's change value is either greater than 3 or below -3
func (db *DB) condition5() ([]string, error) {
	var tickers []string

	if !isValid("5") {
		return tickers, nil
	}
	err := db.DB.Table("dailybars").
		Select("ticker").
		Where("(change > 3 OR change < 3)").Group("ticker").Having("COUNT(*) > 99 ").Scan(&tickers).
		Error
	if err != nil {
		log.Println("Error when find ticker has change greater than 100", err)
		return tickers, err
	}
	return tickers, err
}

// 6 Ticker name must also be there in input_ticker.txt file
func (db *DB) condition6() ([]string, error) {
	var tickers []string
	if !isValid("6") {
		return tickers, nil
	}
	file, err := os.Open("input_ticker.txt")
	if err != nil {
		log.Println("Error when get ticker from text file", err.Error())
		return tickers, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		tickers = append(tickers, scanner.Text())
	}

	return tickers, err
}
