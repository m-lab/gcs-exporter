// Copyright 2020 gcs-exporter Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//////////////////////////////////////////////////////////////////////////////

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
	collectTime time.Duration
)

func init() {
	flag.Var(&sources, "source", "gs://<bucket>")
	flag.DurationVar(&collectTime, "time", 4*time.Hour+10*time.Minute, "Run collections at given UTC time daily.")
	log.SetFlags(log.LUTC | log.Lshortfile | log.Ltime | log.Ldate)
}

var (
	mainCtx, mainCancel = context.WithCancel(context.Background())
)

// nextUpdateTime returns the next time that the collector should run Update().
// The time is aligned on period with the offset added. Period is typically 24h,
// and offset is the time within the period after which all data is available.
//
// Using the 'next' update time, the caller should calculate a wait time using
// something like `next.Sub(now)` and the "date to process" using `next.Add(-period)`.
func nextUpdateTime(now time.Time, period, offset time.Duration) time.Time {
	// Align current time with given period and offset.
	aligned := now.Truncate(period).Add(offset)
	if now.After(aligned) {
		// We've already passed the aligned time today. So, adjust aligned to next period.
		return aligned.Add(period)
	}
	// The aligned time is already in the future, so just return that.
	return aligned
}

var (
	opts []option.ClientOption
)

func main() {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Failed to parse args")

	client, err := storage.NewClient(mainCtx, opts...)
	rtx.Must(err, "Failed to create client")

	buckets := map[string]gcs.Walker{}
	for _, s := range sources {
		buckets[s] = storagex.NewBucket(client.Bucket(s))
	}

	nextUpdate := nextUpdateTime(time.Now().UTC(), 24*time.Hour, collectTime)
	// Initialize the collector starting two days in the past. The loop below will
	// get the most recent day on the first round.
	c := gcs.NewCollector(buckets, nextUpdate.Add(-48*time.Hour))
	prometheus.MustRegister(c)

	srv := prometheusx.MustServeMetrics()
	defer srv.Close()

	for {
		now := time.Now().UTC()
		delay := nextUpdate.Sub(now)
		priorDay := nextUpdate.Add(-24 * time.Hour)

		log.Printf("Sleeping: %s until next update for %s", delay, priorDay)

		select {
		case <-mainCtx.Done():
			return
		case <-time.After(delay):
			// NOTE: Update should only run once a day.
			// NOTE: ignore Update errors.
			c.Update(mainCtx, priorDay)
			// Update nextUpdate to tomorrow.
			nextUpdate = nextUpdate.Add(24 * time.Hour)
		}
	}
}
