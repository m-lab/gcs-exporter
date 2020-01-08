package main

import (
	"testing"

	"github.com/m-lab/go/flagx"
	"google.golang.org/api/option"
)

func Test_main(t *testing.T) {
	// Run once with a cancelled main context to return immediately.
	mainCancel()
	opts = []option.ClientOption{option.WithoutAuthentication()}
	sources = flagx.StringArray{"fake-gcs-bucket"}
	main()
}
