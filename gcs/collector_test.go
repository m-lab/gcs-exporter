package gcs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/go/prometheusx/promtest"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/go/storagex"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeWalker struct {
	dirs map[string][]string
	walk map[string][]*storagex.Object
}

func (f *fakeWalker) Walk(ctx context.Context, prefix string, visit func(o *storagex.Object) error) error {
	objs := f.walk[prefix]
	if objs == nil {
		return fmt.Errorf("Unknown prefix")
	}
	for _, o := range objs {
		visit(o)
	}
	return nil
}

func (f *fakeWalker) Dirs(ctx context.Context, prefix string) ([]string, error) {
	dirs := f.dirs[prefix]
	if dirs == nil {
		return nil, fmt.Errorf("Unknown prefix")
	}
	return dirs, nil
}

func TestCollector_Registration(t *testing.T) {
	tests := []struct {
		name      string
		yesterday time.Time
		buckets   map[string]Walker
		wantErr   bool
	}{
		{
			name:      "success",
			yesterday: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			buckets: map[string]Walker{
				"fake-bucket-name": &fakeWalker{
					dirs: map[string][]string{
						// Root entry.
						"":     []string{"ndt/"},
						"ndt/": []string{"ndt/pcap/", "ndt/tcpinfo/"},
					},
					walk: map[string][]*storagex.Object{
						"ndt/pcap/2019/01/01/": []*storagex.Object{
							&storagex.Object{
								ObjectHandle: &storage.ObjectHandle{},
								ObjectAttrs: &storage.ObjectAttrs{
									Size: 10,
								},
							},
						},
						"ndt/tcpinfo/2019/01/01/": []*storagex.Object{},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCollector(tt.buckets, tt.yesterday)
			err := prometheus.Register(c)
			rtx.Must(err, "Failed to register gcs collector")

			if !promtest.LintMetrics(t) {
				t.Errorf("Metrics lint failed")
			}
		})
	}
}
