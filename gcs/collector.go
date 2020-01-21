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

package gcs

import (
	"context"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/go/storagex"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	lastCollectionDuration = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gcs_collect_time_seconds",
			Help: "Most recent time to list a given bucket/experiment/datatype",
		},
		[]string{"bucket", "experiment", "datatype"},
	)
	lastUpdateDuration = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "gcs_update_time_seconds",
			Help: "Most recent time to update all metrics",
		},
	)
	updateErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gcs_update_errors_total",
			Help: "Number of update errors",
		},
		[]string{"type"},
	)
	archiveFiles = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gcs_archive_files_total",
			Help: "GCS archive file count",
		},
		[]string{"bucket", "experiment", "datatype"},
	)
	archiveBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gcs_archive_bytes_total",
			Help: "GCS archive file count",
		},
		[]string{"bucket", "experiment", "datatype"},
	)
)

// Walker interface.
type Walker interface {
	// Walk recursively visits objects at pathPrefix. See also storagex.Bucket.Walk.
	Walk(ctx context.Context, pathPrefix string, visit func(o *storagex.Object) error) error
	// Dirs returns "directory" names found at prefix. See also storagex.Bucket.Dirs.
	Dirs(ctx context.Context, prefix string) ([]string, error)
}

type labels struct {
	Bucket     string
	Experiment string
	Datatype   string
}

type counts struct {
	files int64
	bytes int64
}

// Update runs the collector query and atomically updates the cached metrics.
// Update is called automatically after the collector is registered.
func Update(ctx context.Context, name string, w Walker, yesterday time.Time) error {
	log.Println("Starting to walk:", yesterday.Format("2006/01/02"))
	start := time.Now()

	metrics, err := collect(ctx, name, w, yesterday)
	for l, c := range metrics {
		archiveFiles.WithLabelValues(l.Bucket, l.Experiment, l.Datatype).Add(float64(c.files))
		archiveBytes.WithLabelValues(l.Bucket, l.Experiment, l.Datatype).Add(float64(c.bytes))
	}

	log.Println("Total time to Update:", time.Since(start))
	lastUpdateDuration.Set(time.Since(start).Seconds())
	return err
}

// TODO: use etl.ExpTypePattern once directory structure is normalized to avoid
// <exp>/<YYYY> dirs which we don't care about.
// datatypePattern matches <experiment>/<datatype>/ directory prefixes.
var datatypePattern = regexp.MustCompile("^[a-z-]+/[a-z][a-z0-9-]+/$")

func getExperimentAndDatatypes(ctx context.Context, bucket string, w Walker) ([]labels, error) {
	// First and second level directories.
	var ret []labels
	first, err := w.Dirs(ctx, "")
	if err != nil {
		updateErrors.WithLabelValues("dirs-first").Inc()
		return nil, err
	}
	for _, experiment := range first {
		second, err := w.Dirs(ctx, experiment)
		if err != nil {
			updateErrors.WithLabelValues("dirs-second").Inc()
			return nil, err
		}
		for _, dtype := range second {
			// datatypePattern selects valid M-Lab datatype directories. This is very
			// M-Lab specific. In particular, the pattern should ignore YYYY directories.
			if datatypePattern.MatchString(dtype) {
				l := labels{
					Bucket:     bucket,
					Experiment: strings.TrimSuffix(experiment, "/"),
					Datatype:   strings.TrimSuffix(strings.TrimPrefix(dtype, experiment), "/"),
				}
				ret = append(ret, l)
			}
		}
	}
	return ret, nil
}

func collect(ctx context.Context, bucket string, w Walker, date time.Time) (map[labels]counts, error) {
	ret := map[labels]counts{}
	m := sync.Mutex{}
	wg := sync.WaitGroup{}

	dtLabels, err := getExperimentAndDatatypes(ctx, bucket, w)
	if err != nil {
		updateErrors.WithLabelValues("get-datatypes").Inc()
		return nil, err
	}
	for _, label := range dtLabels {
		// Collect directories in parallel.
		wg.Add(1)
		go func(w Walker, l labels) {
			files, bytes, err := count(ctx, w, l, date)
			if err != nil {
				// Not ideal, but not fatal.
				updateErrors.WithLabelValues("count").Inc()
				log.Println("Failure counting:", l, "error:", err)
			}
			m.Lock()
			v := ret[l]
			v.files += files
			v.bytes += bytes
			ret[l] = v
			m.Unlock()
			wg.Done()
		}(w, label)
	}
	wg.Wait()
	return ret, nil
}

func count(ctx context.Context, w Walker, label labels, date time.Time) (int64, int64, error) {
	var files int64
	var bytes int64
	prefix := label.Experiment + "/" + label.Datatype + "/" + date.Format("2006/01/02/")
	start := time.Now()

	// Count number of files.
	err := w.Walk(ctx, prefix, func(o *storagex.Object) error {
		// TODO: should be possible to collect min/max creation timestamps to observe transfer start/stop times.
		// TODO: check that files match archives and count non-matching filenames.
		// TODO: could generate histogram of archive mtimes.
		bytes += o.Size
		files++
		return nil
	})

	// Record & report runtime.
	lastCollectionDuration.WithLabelValues(
		label.Bucket, label.Experiment, label.Datatype).Set(time.Since(start).Seconds())
	log.Printf("Finished walking: %-32s %0.6f %5d %5d", prefix, time.Since(start).Seconds(), files, bytes)
	return files, bytes, err
}
