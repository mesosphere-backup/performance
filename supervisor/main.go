package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/mesosphere/journald-scale-test/supervisor/api"
)

func main() {
	logrus.Fatal(api.StartWebServer())
}
