package utils

import (
	"archive/zip"
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
	"github.com/jmoiron/sqlx"
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

type DB struct {
	*sqlx.DB
}

func InitDB() (*DB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 port=5433 user=dev_user dbname=transaction_db password=Dev$54321")
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}
	db.SetMaxOpenConns(500)
	db.SetMaxIdleConns(20000)
	db.SetConnMaxLifetime(60 * time.Minute)

	d := &DB{
		db,
	}
	return d, nil
}

func Unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	os.MkdirAll(dest, 0755)

	// Closure to address file descriptors issue with all the deferred .Close() methods
	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, f.Name)

		// Check for ZipSlip (Directory traversal)
		if !strings.HasPrefix(path, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path: %s", path)
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			os.MkdirAll(filepath.Dir(path), f.Mode())
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func ClearFile(specUrl string) error {
	absPath1, _ := filepath.Abs("../short_sale/extract/" + specUrl + ".txt")
	absPath2, _ := filepath.Abs("../short_sale/" + specUrl + ".zip")

	e := os.Remove(absPath1)
	if e != nil {
		log.Println(e)
	}
	e = os.Remove(absPath2)
	if e != nil {
		log.Println(e)
	}
	return e
}

func MainFunc() {
	if len(os.Args) == 1 {
		log.Println("please enter specUrl")
		return
	}
	specUrl := os.Args[1]

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

	db, err := InitDB()
	if err != nil {
		log.Fatalln("Can't open db", err)
	} else {
		log.Println("db connected ...")
	}
	defer db.Close()

	absPath, _ := filepath.Abs("../short_sale/extract/" + specUrl + ".txt")
	fmt.Println("==============begin read line==============")

	err = ReadFileLineByLine(absPath, specUrl, db)
	if err != nil {
		log.Println("can not read file")
	}

	// ParseData(text, specUrl, db)

	err = ClearFile(specUrl)
	if err != nil {
		log.Println(err)
	}
}

func ParseData(text string, arr map[string][]Short_Sale_Transactions, specUrl string) map[string][]Short_Sale_Transactions {

	fields := strings.Split(text, "|")
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
	return arr
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

	for scanner.Scan() {
		mapShortSale = ParseData(scanner.Text(), mapShortSale, specUrl)
	}

	for date, _ := range mapShortSale {
		err := createShortSaleTable(db, date)
		if err != nil {
			return err
		}
	}

	inserter := workerpool.New(500)
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

func createShortSaleTable(db *DB, date string) error {
	queryStr := fmt.Sprintf("%s%s%s", "CREATE TABLE IF NOT EXISTS short_sale_", date, `(
		date date,
		marketcenter text,
		symbol text,
		tm text,
		shorttype text,
		size integer,
		price real,
		filename text,
		)`)
	_, err := db.Exec(queryStr)
	if err != nil {
		return err
	}

	return nil
}

func insertData(db *DB, arr []Short_Sale_Transactions, date string) error {
	dateTable := strings.Replace(date, "-", "_", 2)
	qry := fmt.Sprintf(`INSERT INTO transactions_%s (date,marketcenter,symbol,tm,shorttype,size,price,filename)
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
			time.Now().Format("15:04:05"),
			1,
		)
		if err != nil {
			log.Println("can not insert data table: ", err)
			errors.Wrap(err, "Cannot add query")
		}
	}

	return nil
}
