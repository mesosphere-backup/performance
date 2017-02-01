package main

import (
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/mesosphere/journald-scale-test/supervisor/api"
	"github.com/mesosphere/journald-scale-test/supervisor/config"
)

func main() {
	cfg, err := config.NewConfig(os.Args)
	if err != nil {
		logrus.Fatalf("Error init config: %s", err)
	}

	interval := cfg.Wait + cfg.CPUUsageInterval
	logrus.Infof("Collecting metrics every %s", interval.String())
	logrus.Infof("Uploading metrics every %s", cfg.UploadInterval.String())

	logrus.Fatal(api.StartWebServer(cfg))
}
