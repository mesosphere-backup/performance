package main

import (
	"context"
	"os"
)

func main() {
	cfg, err := NewConfig(os.Args)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	StartWatcher(ctx, cfg, cancel)
}
