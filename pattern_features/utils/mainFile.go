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
	Value20DaysChangePct string
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
	// allTickers := []string{"AAPL", "SPY"}
	start, _ := time.Parse("2006-01-02", os.Args[2])
	end, _ := time.Parse("2006-01-02", os.Args[1])
	// Create new table average_volumes
	db.AutoMigrate(&PatternFeature{})
	wp := workerpool.New(100)
	for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
		t := t
		wp.Submit(func() {
			if t.Weekday() == 0 || t.Weekday() == 6 {
				log.Println("-----t", t)
				// continue
			} else {
				log.Println("-----start end", start, end)
				last20Days := t.AddDate(0, 0, -20).Format("2006-01-02")
				last200Days := t.AddDate(0, 0, -200).Format("2006-01-02")
				log.Println("-----", start, last20Days, last200Days)
				err = db.PatternFeature(allTickers, t.Format("2006-01-02"), last20Days, last200Days)
				if err != nil {
					log.Fatal("Error when get v from dailybars", err)
					return
				}
			}
		})
	}
	wp.StopWait()
	log.Println("done")
}
func (db *DB) PatternFeature(tickers []string, start, last20Days, last200Days string) error {
	var patternFeatureRecords []*PatternFeature
	wp := workerpool.New(100)

	for k, ticker := range tickers {
		ticker := ticker
		wp.Submit(func() {
			var dailyBar DailyBar
			var last20DaysDailyBar DailyBar
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
			err = db.DB.Raw("select o,c from dailybars where ticker = ? and date=?", ticker, last20Days).Scan(&last20DaysDailyBar).Error
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
			var value20DaysChangePct float64
			log.Println("---------0", dailyBar)
			log.Println("---------1", last20DaysDailyBar)
			log.Println("---------2", len(last200DaysDailyBar))
			log.Println("---------3", closePriceSum, averagePrices)

			//1. c_o : Value would be either 0 or 1 , if today’s close is greater than today's open its 1 else 0
			if dailyBar.C > dailyBar.O {
				co = true
			}

			//2. 20_days_change_pct : change in closing price in percentage from 20 days ago close to todays close (formula is 20 day's close - today's close / 1
			value20DaysChangePct = ((dailyBar.C - last20DaysDailyBar.C) / last20DaysDailyBar.C) * 100
			log.Println("---------4", dailyBar.C, last20DaysDailyBar.C)
			if dailyBar.C == 0 || last20DaysDailyBar.C == 0 {
				value20DaysChangePct = 0
			}
			//3. above_200ma : value would be 0 or 1, 0 when its below or less than last 200 days average closing price else 1
			if dailyBar.C > averagePrices {
				above200Ma = true
			}
			patternFeatureRecord := &PatternFeature{
				Ticker:               ticker,
				Date:                 start,
				CO:                   co,
				Value20DaysChangePct: fmt.Sprintf("%f", value20DaysChangePct),
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
	chunk := 10000
	i := 0
	j := len(patternFeatureRecords)
	for i = 0; i < j; i += chunk {
		start := i
		end := i + chunk
		if j-i < chunk {
			end = j
		}
		temporary := patternFeatureRecords[start:end]
		log.Println("lllllll", start, "---", end)
		err := db.Create(temporary).Error
		if err != nil {
			fmt.Println(err)
		}
		// do whatever
	}

	return nil
}
