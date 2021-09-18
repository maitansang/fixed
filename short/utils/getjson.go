package utils

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

var myClient = &http.Client{Timeout: 60 * time.Second}

func getJson(url string, target interface{}) error {
	var r *http.Response
	var err error
	r, err = myClient.Get(url)
	var i int64
	for ; err != nil; r, err = myClient.Get(url) { //|| r.StatusCode != 200
		time.Sleep(1 * time.Second)
		i++
		log.Println("ERROR GET JSON !!!!!!!!!!!!!!!! RETRYING ", i, err, url)
	}
	defer r.Body.Close()
	//fmt.Println("getJson", url, r.StatusCode)
	return json.NewDecoder(r.Body).Decode(target)
}
