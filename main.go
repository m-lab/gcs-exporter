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
	"sync"
	"time"

	"github.com/m-lab/gcs-exporter/gcs"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/go/storagex"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
)

var (
	sources      flagx.StringArray
	collectTimes flagx.DurationArray
)

func init() {
	flag.Var(&sources, "source", "gs://<bucket>")
	flag.Var(&collectTimes, "time", "Run collections at given UTC time daily.")
	log.SetFlags(log.LUTC | log.Lshortfile | log.Ltime | log.Ldate)
}

var (
	opts                []option.ClientOption
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

// updateForever runs the gcs.Update on the given bucket at the given collect time every day.
func updateForever(ctx context.Context, wg *sync.WaitGroup, bucket string, walker gcs.Walker, collect time.Duration) {
	defer wg.Done()

	nextUpdate := nextUpdateTime(time.Now().UTC(), 24*time.Hour, collect)
	// Initialize the bucket starting two days in the past. The loop below will
	// get the most recent day on the first round.
	gcs.Update(mainCtx, bucket, walker, nextUpdate.Add(-48*time.Hour))

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
			gcs.Update(mainCtx, bucket, walker, priorDay)
			// Update nextUpdate to tomorrow.
			nextUpdate = nextUpdate.Add(24 * time.Hour)
		}
	}
}

var logFatal = log.Fatal

func main() {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Failed to parse args")

	if len(sources) != len(collectTimes) {
		logFatal("Must provide same number of sources as collection times.")
	}

	srv := prometheusx.MustServeMetrics()
	defer srv.Close()

	client, err := storage.NewClient(mainCtx, opts...)
	rtx.Must(err, "Failed to create client")

	wg := sync.WaitGroup{}
	for i, t := range collectTimes {
		wg.Add(1)
		walker := storagex.NewBucket(client.Bucket(sources[i]))
		go updateForever(mainCtx, &wg, sources[i], walker, t)
	}
	wg.Wait()
}
