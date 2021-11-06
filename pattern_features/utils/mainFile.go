package utils
import (
	"log"
	"time"

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

	
}