package gcs

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/go/prometheusx/promtest"
	"github.com/m-lab/go/storagex"
)

type fakeWalker struct {
	dirs      map[string][]string
	dirsCalls int
	walk      map[string][]*storagex.Object
	walkCalls int
	m         sync.Mutex
}

func (f *fakeWalker) Walk(ctx context.Context, prefix string, visit func(o *storagex.Object) error) error {
	f.m.Lock()
	f.walkCalls++
	f.m.Unlock()
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
	f.m.Lock()
	f.dirsCalls++
	f.m.Unlock()
	dirs := f.dirs[prefix]
	if dirs == nil {
		return nil, fmt.Errorf("Unknown prefix")
	}
	return dirs, nil
}

func TestUpdate(t *testing.T) {
	tests := []struct {
		name      string
		yesterday time.Time
		bucket    string
		walker    Walker
		wantErr   bool
	}{
		{
			name:      "success",
			yesterday: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			bucket:    "fake-bucket-name",
			walker: &fakeWalker{
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
		{
			name:      "error-dirs-first",
			yesterday: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			bucket:    "fake-bucket-name",
			walker: &fakeWalker{
				dirs: map[string][]string{
					// Missing root record generates a fake error.
				},
			},
			wantErr: true,
		},
		{
			name:      "error-dirs-second",
			yesterday: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			bucket:    "fake-bucket-name",
			walker: &fakeWalker{
				dirs: map[string][]string{
					"": []string{"ndt/"},
					// Missing ndt record generates a fake error.
				},
			},
			wantErr: true,
		},
		{
			name:      "error-walk",
			yesterday: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			bucket:    "fake-bucket-name",
			walker: &fakeWalker{
				dirs: map[string][]string{
					"":     []string{"ndt/"},
					"ndt/": []string{"ndt/pcap/", "ndt/tcpinfo/"},
				},
				walk: map[string][]*storagex.Object{
					// Missing record generates a fake error. This error does not propagate.
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			err := Update(ctx, tt.bucket, tt.walker, tt.yesterday)

			if tt.wantErr != (err != nil) {
				t.Errorf("Failed to register gcs collector %s", err)
			}
			if !promtest.LintMetrics(t) {
				t.Errorf("Metrics lint failed")
			}
			f := tt.walker.(*fakeWalker)
			if !tt.wantErr && (f.walkCalls == 0 || f.dirsCalls == 0) {
				t.Errorf("Unexpected number of calls to Walk(%d) or Dirs(%d)", f.walkCalls, f.dirsCalls)
			}
		})
	}
}
