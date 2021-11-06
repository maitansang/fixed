package utils

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gammazero/workerpool"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type PatternFeature struct {
	Ticker               string
	Date                 string
	CO                   bool
	Value14DaysChangePct string
	Above200Ma           bool
}
type DailyBar struct {
	O float64
	C float64
}
type DB struct {
	*gorm.DB
}

func (db DB) getAllTicker() ([]string, error) {
	var tickers []string
	if err := db.DB.Table("tickers").
		Select("symbol").Scan(&tickers).Error; err != nil {
		log.Println("Error when get all ticker ", err)
		return nil, err
	}
	return removeDuplicateValues(tickers), nil
}
func removeDuplicateValues(intSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}

	// If the key(values of the slice) is not equal
	// to the already present value in new slice (list)
	// then we append it. else we jump on another element.
	for _, entry := range intSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

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
	// defer sqlDB.Close()
	return DB, nil
}
func MainFunc() {
	db, err := InitDB()
	if err != nil {
		log.Println("can not init db", err)
		return
	}

	sqlDB, err := db.DB.DB()
	if err != nil {
		log.Println("Error when init sql db")
		return
	}
	defer sqlDB.Close()
	allTickers, err := db.getAllTicker()
	if err != nil {
		log.Println("Error when get all ticker", err)
		return
	}
	// allTickers = []string{"AAPL"}
	start, _ := time.Parse("2006-01-02", os.Args[1])
	last14Days := start.AddDate(0, 0, -14)
	last200Days := start.AddDate(0, 0, -200)

	// Create new table average_volumes
	db.AutoMigrate(&PatternFeature{})
	err = db.PatternFeature(allTickers, start.Format("2006-01-02"), last14Days.Format("2006-01-02"), last200Days.Format("2006-01-02"))
	if err != nil {
		log.Fatal("Error when get v from dailybars", err)
		return
	}
	log.Println("done")
}
func (db *DB) PatternFeature(tickers []string, start, last14Days, last200Days string) error {
	var patternFeatureRecords []*PatternFeature
	wp := workerpool.New(100)

	for k, ticker := range tickers {
		ticker := ticker
		wp.Submit(func() {
			var dailyBar DailyBar
			var last14DaysDailyBar DailyBar
			var last200DaysDailyBar []DailyBar
			var closePriceSum float64
			var averagePrices float64

			if k == len(tickers) {
				return
			}
			err := db.DB.Raw("select o,c from dailybars where ticker = ? and date=?", ticker, start).Scan(&dailyBar).Error
			if err != nil {
				log.Fatal("Error when get v from dailybars", err)
				return
			}
			err = db.DB.Raw("select o,c from dailybars where ticker = ? and date=?", ticker, last14Days).Scan(&last14DaysDailyBar).Error
			if err != nil {
				log.Fatal("Error when get v from dailybars", err)
				return
			}
			err = db.DB.Raw("select o,c from dailybars where ticker = ?  and date>? and date<=?", ticker, last200Days, start).Scan(&last200DaysDailyBar).Error
			if err != nil {
				log.Fatal("Error when get v from dailybars", err)
				return
			}

			for _, v := range last200DaysDailyBar {
				closePriceSum = closePriceSum + v.C
			}
			averagePrices = closePriceSum / float64(len(last200DaysDailyBar))

			above200Ma := false
			co := false
			var value14DaysChangePct float64
			log.Println("---------0", dailyBar)
			log.Println("---------1", last14DaysDailyBar)
			log.Println("---------2", len(last200DaysDailyBar))
			log.Println("---------3", averagePrices)

			//1. c_o : Value would be either 0 or 1 , if today’s close is greater than today's open its 1 else 0
			if dailyBar.C > dailyBar.O {
				co = true
			}

			//2. 14_days_change_pct : change in closing price in percentage from 14 days ago close to todays close (formula is 14 day's close - today's close / 1
			value14DaysChangePct = (last14DaysDailyBar.C - dailyBar.C) / last14DaysDailyBar.C

			//3. above_200ma : value would be 0 or 1, 0 when its below or less than last 200 days average closing price else 1
			if dailyBar.C > averagePrices {
				co = true
			}
			patternFeatureRecord := &PatternFeature{
				Ticker:               ticker,
				Date:                 start,
				CO:                   co,
				Value14DaysChangePct: fmt.Sprintf("%f", value14DaysChangePct),
				Above200Ma:           above200Ma,
			}
			log.Println("===========0", patternFeatureRecord)
			patternFeatureRecords = append(patternFeatureRecords, patternFeatureRecord)
		})

	}
	wp.StopWait()

	//Clear old data
	db.Where("date = ?", start).Delete(PatternFeature{})
	fmt.Println("len(averageVolumeRecords)", len(patternFeatureRecords))
	chunk := 40000
	i := 0
	j := len(patternFeatureRecords)
	for i = 0; i < j; i += chunk {
		start := i
		end := i+ chunk
		if i> j {
			end = j
		}
		temporary := patternFeatureRecords[start : end]
		log.Println("lllllll", i, "---", len(temporary))
		err := db.Create(temporary).Error
		if err != nil {
			fmt.Println(err)
		}
		// do whatever
	}

	return nil
}
