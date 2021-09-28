package main

import (
	"log"
	"os/exec"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"gopkg.in/robfig/cron.v2"
)

func initDB() (*sqlx.DB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 user=postgres dbname=stockmarket password=P`AgD!9g!%~hz3M< sslmode=disable")
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}

	return db, nil
}

func main() {
	var (
		start   string
		end     string
		specify string
	)

	db, err := initDB()
	if err != nil {
		log.Fatalln("Cannot init db", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	c := cron.New()
	c.Start()
	c.AddFunc("TZ=America/New_York 00 30 22 * * *", func() {
		db.Exec("SELECT pg_terminate_backend(pid)	FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = 'stockmarket' AND state = 'idle'")
		loc, _ := time.LoadLocation("America/New_York")
		currentTime := time.Now().In(loc)
		start = currentTime.AddDate(0, 0, -1).Format("2006-01-02")
		end = currentTime.Format("2006-01-02")
		specify = currentTime.AddDate(0, 0, -30).Format("2006-01-02")
		log.Println("Wait a minute to execute the script!")
		cmd := exec.Command("sh", "run.sh", start, end, specify)
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Update successful!")
	})
	c.Start()
	wg.Wait()

}
