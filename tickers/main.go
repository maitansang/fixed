package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type DB struct {
	*sqlx.DB
}

func InitDB() (*DB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 user=postgres dbname=stockmarket password=P`AgD!9g!%~hz3M< sslmode=disable")
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}
	db.SetMaxOpenConns(250)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(60 * time.Minute)
	d := &DB{
		db,
		//&sync.Mutex{},
	}
	// _, err = d.Exec(`CREATE TABLE IF NOT EXISTS tickers(
	// 	id SERIAL PRIMARY KEY,
	// 	symbol text,
	// 	name text,
	// 	exchange text,
	// 	marketcap bigint,
	// 	sector text,
	// 	industry text,
	// 	tags text [],
	// 	url text,
	// 	UNIQUE(ticker,exchange)
	// )`)

	// if err != nil {
	// 	return nil, errors.Wrap(err, "create table:")
	// }

	return d, nil
}

func main() {
	db, err := InitDB()
	if err != nil {
		log.Fatalln("Cant open db", err)
	}
	defer db.Close()

	// _, err = db.Exec("DELETE FROM tickers")
	// if err != nil {
	// 	log.Fatalln("Cant delete tickers")
	// }

	log.Println(db.getTickerList())

}

const URL_TICKERS = `https://api.polygon.io/v2/reference/tickers?sort=ticker&market=stocks&perpage=50&page=%d&apiKey=wSriypADR8wfUaoyqqaZj_7pMDdRMp1p`
const URL_TICKERS_V3 = `https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&sort=ticker&order=asc&limit=1000`
const API_KEY = `P81v0A4wZ9TpsZbzTeNulj8BYkQWFCSR`

type TickerData struct {
	Ticker   string `json:"ticker"`
	Exchange string `json:"primary_exchange"`
}

func (db DB) getTickerList() error {
	all := struct {
		sync.RWMutex
		data []TickerData
	}{
		RWMutex: sync.RWMutex{},
		data:    []TickerData{},
	}
	//var all []string
	//wp := workerpool.New(20)
	end := false
	nextUrl := URL_TICKERS_V3
	for i := 1; !end; i++ {
		//wp.Submit(func() {
		res, newNextUrl, err := db.getTickerListPage(nextUrl)
		if err != nil {
			log.Println("error json ", err)
		}
		//fmt.Println("res=", res)
		//all.RWMutex.Lock()
		all.data = append(all.data, res...)
		log.Println("Got", len(res))
		if newNextUrl == "" {
			end = true
		}
		nextUrl = newNextUrl
		//all.RWMutex.Unlock()
		//})
	}
	//fmt.Println("all=", all.data, len(all.data))
	//tx, err := db.Begin()
	// if err != nil {
	// 	return errors.Wrap(err, "begin transcation")
	// }

	// fmt.Printf("data %+v, %s\n", all.data[0], nextUrl)
	// for _, ticker := range all.data {
	// 	fmt.Println("inserting", ticker)
		// _, err := db.Exec("INSERT INTO tickers(ticker,exchange) VALUES ($1,$2)", ticker.Ticker, ticker.Exchange)
		// if err != nil {
		// 	log.Println("insert", err)
		// }
	// }
	//err = tx.Commit()

	return nil
}

// type TickerRec struct {
// 	Ticker string `json:"ticker"`
// }

type Page struct {
	Tickers []TickerData `json:"results"`
	NextURL string `json:"next_url"`
}

// var nextPageReference = "https://api.polygon.io:443/v3/reference/tickers?cursor=%s"

func (db DB) getTickerListPage(nextURL string) ([]TickerData, string, error) {
	url := nextURL + "&apikey="+API_KEY
	page := Page{}
	err := getJson(url, &page)
	if err != nil {
		return []TickerData{}, "",  errors.Wrap(err, "can't get json "+url)
	}
	// res := []TickerData{}
	// for _, r := range page.Tickers {
	// 	res = append(res, r)
	// 	//res = append(res, strings.Replace(r.Ticker, ` `, `.`, -1))
	// }
	if len(page.NextURL) != 0 && page.NextURL[26:30] == ":443" {
		page.NextURL = page.NextURL[:26] + page.NextURL[30:]
	}
	return page.Tickers, page.NextURL, nil
}

var myClient = &http.Client{Timeout: 60 * time.Second}

func getJson(url string, target interface{}) error {
	var r *http.Response
	var err error
	r, err = myClient.Get(url)
	var i int64
	for ; (err != nil) && (i < 100); r, err = myClient.Get(url) { //|| r.StatusCode != 200
		time.Sleep(1 * time.Second)
		i++
		log.Println("!!!!!!!!!!!!!!!! RETRYING ", i, err)
	}
	defer r.Body.Close()
	//fmt.Println("getJson", url, r.StatusCode)
	return json.NewDecoder(r.Body).Decode(target)
}
