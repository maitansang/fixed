package utils

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gammazero/workerpool"
	// "github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type DB struct {
	*gorm.DB
}
type AverageVolume struct {
	// ID            string `gorm:"primaryKey"`
	Ticker        string
	AverageVolume float64
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
	// defer sqlDB.Close()
	return DB, nil
}

func (db DB) getAllTicker() ([]string, error) {
	var tickers []string
	if err := db.DB.Table("tickers").
		Select("symbol").Scan(&tickers).Error; err != nil {
		log.Println("Error when get all ticker ", err)
		return nil, err
	}
	fmt.Println("1111111-",len(tickers))
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
	fmt.Println("1111111",len(allTickers))
	if err != nil {
		log.Println("Error when get all ticker", err)
		return
	}
	// log.Println("------0", allTickers)
	//Handle time
	// currentTime := time.Now()

	// start := currentTime.Format("2006-01-02")
	// end := currentTime.AddDate(0, 0, -30).Format("2006-01-02")
	start, _ := time.Parse("2006-01-02", os.Args[1])
	end := start.AddDate(0, 0, -30)
	fmt.Println("===========0", start.Format("2006-01-02"), end.Format("2006-01-02"))
	err = db.AverageVolume(allTickers, start.Format("2006-01-02"), end.Format("2006-01-02"))
	if err != nil {
		log.Fatal("Error when get v from dailybars", err)
		return
	}
	log.Println("======done")
}

func (db *DB) AverageVolume(tickers []string, start, end string) error {
	// var averageVolumeRecords []*AverageVolume
	wp := workerpool.New(10)

	for k, ticker := range tickers {

		wp.Submit(func() {
			var volumes []string
			var volumesSum float64
			var averageVolumes float64
			if k == len(tickers){
				return
			}
			err := db.DB.Raw("select v from dailybars where ticker = ? and date>? and date<=?", ticker, end, start).Scan(&volumes).Error
			if err != nil {
				log.Fatal("Error when get v from dailybars", err)
				return 
			}
			for _, v := range volumes {
				if parseFloatVolume, err := strconv.ParseFloat(v, 32); err == nil {
					volumesSum = volumesSum + parseFloatVolume
				}
			}
			averageVolumes = volumesSum / 30
			fmt.Println("--------", averageVolumes)
			averageVolumeRecord := &AverageVolume{
				// ID:            uuid.NewString(),
				Ticker:        ticker,
				AverageVolume: averageVolumes,
			}
			if db.Model(&AverageVolume{}).Where("ticker = ?", ticker).Updates(&averageVolumeRecord).RowsAffected == 0 {
				db.Create(&averageVolumeRecord)
			}
		})
		
	}
	wp.StopWait()

	return nil
}
