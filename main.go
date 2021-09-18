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
		start string
		end   string
	)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	c := cron.New()
	c.Start()
	c.AddFunc("TZ=America/New_York 00 22 * * * *", func() {
		currentTime := time.Now()
		start = currentTime.AddDate(0, 0, -1).Format("2006-01-02")
		end = currentTime.Format("2006-01-02")
		cmd := exec.Command("sh", "run.sh", start, end)
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
	})
	c.Start()
	wg.Wait()

}
