package main

import (
	"os"
	"fmt"
	"context"
)

func main() {
	cfg, err := NewConfig(os.Args)
	if err != nil {
		panic(fmt.Sprint("Unable to load config: %s", err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	StartWatcher(ctx, cfg, cancel)
}
