package main

import (
	"context"

	"github.com/mesosphere/journald-scale-test/supervisor/watch"
)

func main() {
	watch.StartWatcher(context.Background())
}
