package utils

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"reflect"
	"time"

	"os"
	"path/filepath"
	"strings"

	"github.com/gammazero/workerpool"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type ShortSale struct {
	ID           string `gorm:"primaryKey;autoIncrement:false"`
	Date         string `json:"date" `
	MarketCenter string `json:"marketcenter" `
	Symbol       string `json:"symbol" `
	Time         string `json:"tm" `
	ShortType    string `json:"shorttype" `
	Size         string `json:"size" `
	Price        string `json:"price" `
	FileName     string `json: "filename"`
	// LinkIndicator string `json:"" `
}

func MainFunc() {
	if len(os.Args) == 1 {
		log.Println("please enter valid year and month (format: YYYY-MM)")
		return
	}

	db, err := InitDB()
	if err != nil {
		log.Fatalln("Can't open db", err)
	} else {
		log.Println("db connected ...")
	}
	sqlDB, err := db.DB.DB()
	if err != nil {
		log.Println("Error when init sql db")
		return
	}
	defer sqlDB.Close()

	date := os.Args[1]
	date = strings.Replace(date, "-", "", 1)
	specPrefix := []string{"FNSQsh%s_1"}
	// specPrefix := []string{"FNSQsh%s_1", "FNSQsh%s_2", "FNSQsh%s_3", "FNSQsh%s_4", "FNQCsh%s", "FNYXsh%s"}

	for _, urlPath := range specPrefix {
		specUrl := fmt.Sprintf(urlPath, date)
		err := ClearFile(specUrl)
		if err != nil {
			log.Println(err)
		}
	}
	// wp := workerpool.New(6)
	for _, prefix := range specPrefix {
		// prefix := prefix

		// wp.Submit(func() {
		specUrl := fmt.Sprintf(prefix, date)

		// err := ClearFile(specUrl)
		// if err != nil {
		// 	log.Println(err)
		// }

		resp, err := http.Get("https://cdn.finra.org/equity/regsho/monthly/" + specUrl + ".zip")
		if err != nil {
			fmt.Printf("err: %s", err)
		}

		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return
		}

		// Create the file
		out, err := os.Create(specUrl + ".zip")
		if err != nil {
			fmt.Printf("err: %s", err)
		}
		defer out.Close()

		// Write the body to file
		_, err = io.Copy(out, resp.Body)

		err = Unzip(specUrl+".zip", "extract/")
		if err != nil {
			log.Println("err when extract ", err)
		}

		absPath, _ := filepath.Abs("../short_sale/extract/" + specUrl + ".txt")

		err = ReadFileLineByLine(absPath, specUrl, db)
		if err != nil {
			log.Println("can not read file")
		}

		err = ClearFile(specUrl)
		if err != nil {
			log.Println(err)
		}
		// })
	}
	// wp.StopWait()
}

func ReadFileLineByLine(nameFile string, specUrl string, db *DB) error {
	var mapShortSale = make(map[string][]ShortSale)

	file, err := os.Open(nameFile)

	if err != nil {
		log.Fatalf("failed to open", err)
	}

	scanner := bufio.NewScanner(file)

	scanner.Split(bufio.ScanLines)
	fmt.Println("==============begin read line==============")

	i := 0
	for scanner.Scan() {
		if i == 0 {
			i++
			continue
		}
		mapShortSale = ParseData(scanner.Text(), mapShortSale, specUrl)
	}
	for date, _ := range mapShortSale {
		// if date == "2021-09-15" {
		// 	log.Fatal("========================date", date)
		// }
		err := createShortSaleTable(db, date)
		if err != nil {
			return err
		}
	}
	// time.AfterFunc(30*time.Second, func() {
	// inserter := workerpool.New(30)
	for date, arr := range mapShortSale {
		// date := date
		// arr := arr
		// inserter.Submit(func() {
		// err := createShortSaleTable(db, date)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		insertData(db, arr, date)
		// })
	}
	// inserter.StopWait()
	// })
	// inserter := workerpool.New(30)
	// for date, arr := range mapShortSale {
	// 	date := date
	// 	arr := arr
	// 	inserter.Submit(func() {
	// 		// err := createShortSaleTable(db, date)
	// 		// if err != nil {
	// 		// 	log.Fatal(err)
	// 		// }
	// 		insertData(db, arr, date)
	// 	})
	// }
	// inserter.StopWait()

	file.Close()
	return err
}

func ParseData(text string, arr map[string][]ShortSale, specUrl string) map[string][]ShortSale {
	fields := strings.Split(text, "|")
	if len(fields) > 7 {
		dateTime, err := time.Parse("20060102", fields[2])
		if err != nil {
			log.Println(err)
		}

		dateString := dateTime.Format("2006-01-02")

		trans := ShortSale{
			ID: uuid.NewString(),
			// Date:         dateString,
			MarketCenter: fields[0],
			Symbol:       fields[1],
			Time:         fields[3],
			ShortType:    fields[4],
			Size:         fields[5],
			Price:        fields[6],
			FileName:     specUrl,
		}

		arr[dateString] = append(arr[dateString], trans)
	}
	return arr
}

func createShortSaleTable(db *DB, date string) error {
	dateTable := strings.Replace(date, "-", "_", 2)

	// Create new table
	log.Println("short_sales", "drop table "+"short_sale"+dateTable)
	if err := db.Migrator().DropTable("short_sales", "short_sale_"+dateTable, "short_sales"+dateTable); err != nil {
		log.Println("error drop table")
		return err
	}
	if err := db.Migrator().CreateTable(&ShortSale{}); err != nil {
		log.Println("error create table")
		return err
	}
	log.Println("rename table short_sales to " + "short_sale_" + dateTable)
	if err := db.Migrator().RenameTable("short_sales", "short_sale_"+dateTable); err != nil {
		log.Println("error rename table")
		return err
	}

	return nil
}

func insertData(db *DB, arr []ShortSale, date string) error {
	dateTable := strings.Replace(date, "-", "_", 2)
	// Create bulk data
	numField := reflect.TypeOf(ShortSale{}).NumField()
	parameters := len(arr) * numField

	loop := (float64(parameters) / float64(65535))

	log.Println("================ numField", numField)
	log.Println("================ parameters", parameters)
	log.Println("================ len(arr)", len(arr))

	calLoop := math.Ceil(loop)
	intLoop := int(calLoop)
	var listStartEndPoint = make(map[int64]int64)
	for i := 0; i < int(calLoop); i += 1 {
		start := (len(arr) / intLoop) * i
		end := (len(arr) / intLoop) * (i + 1)
		if (i + 1) >= intLoop {
			end = len(arr)
		}
		listStartEndPoint[int64(start)] = int64(end)
		// existTable := db.Migrator().HasTable("short_sale_" + dateTable)
		// if existTable == true {
		// } else {
		// 	if err := db.Migrator().CreateTable(&ShortSale{}); err != nil {
		// 		log.Println("error create table")
		// 		return err
		// 	}
		// 	if err := db.Migrator().RenameTable("short_sales", "short_sale_"+dateTable); err != nil {
		// 		log.Println("error rename table")
		// 		return err
		// 	}
		// }
		// err := db.Table("short_sale_" + dateTable).Create(arr[start:end]).Error
		// if err != nil {
		// 	log.Println("================ err existTable", err, existTable)
		// 	log.Fatal(err)
		// }
		// log.Println("================ numField", numField)
		// log.Println("================ parameters", parameters)
		// log.Println("================ len(arr)", len(arr))
		// log.Println("start of end ", start, end)
		// log.Println("value of i ", i)
	}
	// log.Fatal("listStartEndPoint", listStartEndPoint)
	for i := 100; i <= len(listStartEndPoint); i += 100 {
		log.Println("================ workerpool i", i)
		insertDatabase := workerpool.New(i)
		for start, end := range listStartEndPoint {
			start := start
			end := end
			// log.Fatal("=====================", start, end)
			insertDatabase.Submit(func() {
				// existTable := db.Migrator().HasTable("short_sale_" + dateTable)
				// if existTable == true {
				// } else {
				// 	if err := db.Migrator().CreateTable(&ShortSale{}); err != nil {
				// 		log.Println("error create table")
				// 		// return err
				// 	}
				// 	if err := db.Migrator().RenameTable("short_sales", "short_sale_"+dateTable); err != nil {
				// 		log.Println("error rename table")
				// 		// return err
				// 	}
				// }
				err := db.Table("short_sale_" + dateTable).Create(arr[start:end]).Error
				if err != nil {
					// log.Println("================ err existTable", err, existTable)
					log.Fatal(err, start, end)
				}
				log.Println("================ numField", numField)
				log.Println("================ parameters", parameters)
				log.Println("================ len(arr)", len(arr))
				log.Println("start of end ", start, end)
				// log.Println("value of i ", i)
			})
		}
		insertDatabase.StopWait()
	}
	return nil
}
