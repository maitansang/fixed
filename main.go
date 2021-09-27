package main

import (
	"log"
	"os/exec"
	"sync"
	"time"

	"gopkg.in/robfig/cron.v2"
)

func main() {
	var (
		start   string
		end     string
		specify string
	)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	c := cron.New()
	c.Start()
	c.AddFunc("TZ=America/New_York 30 22 * * * *", func() {
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
