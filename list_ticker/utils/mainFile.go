package utils

import (
	"fmt"
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)
type DB struct {
	*gorm.DB
}

func InitDB() (*DB, error) {
	// handle db
	dsn := "host=52.116.150.66 user=dev_user password=Dev$54321 dbname=transaction_db port=5433 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Println("can not open db")
	}
	DB := &DB{
		db,
	}
	return DB, nil
}
func MainFunc() {
	db, err := InitDB()
	if err !=nil {
		log.Println("can not init db", err)
	}
	fmt.Println("Ok")
}
