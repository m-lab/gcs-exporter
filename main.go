package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/m-lab/gcs-exporter/gcs"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/go/storagex"

	"cloud.google.com/go/storage"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	sources     flagx.StringArray
	prefix      string
	collectTime = flagx.Time{Hour: 4, Minute: 10}
)

func init() {
	flag.Var(&sources, "source", "gs://<bucket>")
	flag.Var(&collectTime, "time", "Run collections at given UTC time daily.")
	log.SetFlags(log.LUTC | log.Lshortfile | log.Ltime | log.Ldate)
}

var (
	mainCtx, mainCancel = context.WithCancel(context.Background())
)

const secInDay = 24 * 60 * 60

func yesterday(now time.Time, offset int) time.Time {
	// Offset additional 24hrs in the past to align on "most recent complete day"
	return now.Add(time.Duration(-offset-secInDay) * time.Second).Truncate(secInDay * time.Second)
}

// untilNextUpdate calculates the duration between current time and the next scheduled update.
func untilNextUpdate(cur time.Time, nextOffsetSec int) time.Duration {
	// Convert next and current times to an offset in seconds.
	curOffsetSec := cur.Hour()*60*60 + cur.Minute()*60 + cur.Second()
	if nextOffsetSec > curOffsetSec {
		// The next time is still in the current day.
		return time.Duration(nextOffsetSec-curOffsetSec) * time.Second
	}
	// We've missed the next time for today, so add nextOffset to the time remaining today.
	return time.Duration(nextOffsetSec+(secInDay-curOffsetSec)) * time.Second
}

func main() {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Failed to parse args")

	client, err := storage.NewClient(mainCtx)
	rtx.Must(err, "Failed to create client")

	buckets := map[string]gcs.Walker{}
	for _, s := range sources {
		buckets[s] = storagex.NewBucket(client.Bucket(s))
	}

	// Calculate static offset to calculate the latest complete day.
	offset := collectTime.Hour*60*60 + collectTime.Minute*60 + collectTime.Second

	// Initialize the collector using data from "yesterday".
	c := gcs.NewCollector(buckets, yesterday(time.Now().UTC(), offset))
	prometheus.MustRegister(c)

	srv := prometheusx.MustServeMetrics()
	defer srv.Close()

	for {
		delay := untilNextUpdate(time.Now().UTC(), offset)
		log.Printf("Sleeping %s until next update at %s", delay, time.Now().UTC().Add(delay))
		select {
		case <-mainCtx.Done():
			return
		case <-time.After(delay):
			// NOTE: ignore Update errors.
			// NOTE: sqhould only update once a day.
			c.Update(mainCtx, yesterday(time.Now().UTC(), offset))
		}
	}
}
