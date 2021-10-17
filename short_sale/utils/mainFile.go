package utils

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"os"
	"path/filepath"
	"strings"

	"github.com/gammazero/workerpool"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type Short_Sale_Transactions struct {
	ID           string `gorm:"primaryKey;autoIncrement:false"`
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
	defer db.Close()

	date := os.Args[1]
	date = strings.Replace(date, "-", "", 1)
	specPrefix := []string{"FNSQsh%s_1", "FNQCsh%s", "FNYXsh%s"}

	wp := workerpool.New(3)
	for _, prefix := range specPrefix {
		prefix := prefix

		wp.Submit(func() {
			specUrl := fmt.Sprintf(prefix, date)

			err := ClearFile(specUrl)
			if err != nil {
				log.Println(err)
			}

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
		})
	}
	wp.StopWait()
}

func ReadFileLineByLine(nameFile string, specUrl string, db *DB) error {
	var mapShortSale = make(map[string][]Short_Sale_Transactions)

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
		err := createShortSaleTable(db, date)
		if err != nil {
			return err
		}
	}

	inserter := workerpool.New(30)
	for date, arr := range mapShortSale {
		date := date
		arr := arr
		inserter.Submit(func() {
			insertData(db, arr, date)
		})
	}
	inserter.StopWait()

	file.Close()
	return err
}

func ParseData(text string, arr map[string][]Short_Sale_Transactions, specUrl string) map[string][]Short_Sale_Transactions {
	fields := strings.Split(text, "|")
	if len(fields) > 7 {
		dateTime, err := time.Parse("20060102", fields[2])
		if err != nil {
			log.Println(err)
		}

		dateString := dateTime.Format("2006-01-02")

		trans := Short_Sale_Transactions{
			ID:           uuid.NewString(),
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
	queryStr := fmt.Sprintf("%s%s%s", "CREATE TABLE IF NOT EXISTS short_sale_", dateTable, `(
		date date,
		marketcenter text,
		symbol text,
		tm text,
		shorttype text,
		size integer,
		price real,
		filename text
		)`)
	_, err := db.Exec(queryStr)
	if err != nil {
		return err
	}

	return nil
}

func insertData(db *DB, arr []Short_Sale_Transactions, date string) error {
	dateTable := strings.Replace(date, "-", "_", 2)
	qry := fmt.Sprintf(`INSERT INTO short_sale_%s (date,marketcenter,symbol,tm,shorttype,size,price,filename)
					VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, dateTable)
	for _, v := range arr {
		_, err := db.Exec(
			qry,
			date,
			v.MarketCenter,
			v.Symbol,
			v.Time,
			v.ShortType,
			v.Size,
			v.Price,
			v.FileName,
		)
		if err != nil {
			log.Println("can not insert data table: ", err)
			errors.Wrap(err, "Cannot add query")
		}
	}

	return nil
}
