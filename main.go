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
	"google.golang.org/api/option"

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

func nextAlignedTime(now time.Time, period, offset time.Duration) (time.Time, time.Duration) {
	// Align current time with given period and offset.
	aligned := now.Truncate(period).Add(offset)
	if now.After(aligned) {
		// We've missed the time today, so calculate delay between now and next period.
		wait := period - now.Sub(aligned)
		next := aligned.Add(wait)
		return next.Add(-period), wait
	}
	// The current time is after the aligned time.
	return aligned.Add(-period), aligned.Sub(now)
}

func main() {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Failed to parse args")

	client, err := storage.NewClient(mainCtx, option.WithoutAuthentication())
	rtx.Must(err, "Failed to create client")

	buckets := map[string]gcs.Walker{}
	for _, s := range sources {
		buckets[s] = storagex.NewBucket(client.Bucket(s))
	}

	// Calculate static time offset used before calculating the latest complete day.
	offset := time.Duration(collectTime.Hour)*time.Hour +
		time.Duration(collectTime.Minute)*time.Minute +
		time.Duration(collectTime.Second)*time.Second

	next, _ := nextAlignedTime(time.Now().UTC(), 24*time.Hour, offset)
	// Initialize the collector starting an additional day in the past.
	c := gcs.NewCollector(buckets, next.Add(-24*time.Hour))
	prometheus.MustRegister(c)

	srv := prometheusx.MustServeMetrics()
	defer srv.Close()

	for {
		next, delay := nextAlignedTime(time.Now().UTC(), 24*time.Hour, offset)
		log.Printf("Sleeping: %s until next update at %s", delay, next)

		select {
		case <-mainCtx.Done():
			return
		case <-time.After(delay):
			// NOTE: ignore Update errors.
			// NOTE: sqhould only update once a day.
			c.Update(mainCtx, next)
		}
	}
}
