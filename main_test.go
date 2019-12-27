package main

import (
	"testing"

	"github.com/m-lab/go/flagx"
)

func Test_main(t *testing.T) {
	// Run once with a cancelled main context to return immediately.
	mainCancel()
	sources = flagx.StringArray{"fake-gcs-bucket"}
	main()
}
