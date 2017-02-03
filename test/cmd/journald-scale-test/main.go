package main

import (
	"flag"

	"github.com/Sirupsen/logrus"
	"github.com/mesosphere/performance/test"
	"github.com/mesosphere/performance/test/suite/journald"
)

var (
	testType      = flag.String("test-type", "constant-rate", "The type of test to run")
	supervisorURL = flag.String("supervisor-url", "http://localhost:9123/incoming", "host:port to POST events to")
	logRate       = flag.Int("log-rate", 1000, "Rate of logs sent to STDOUT in lines per second")
	stdErr        = flag.Bool("stderr", false, "Sends log lines to STDERR in conjunction with STDOUT")
	testDuration  = flag.Int("duration", 60, "Test duration in seconds")

	log = logrus.WithFields(logrus.Fields{
		"suite": "jounrald-test-exec",
	})
)

func main() {
	flag.Parse()

	journaldSuite, err := journald.NewTestSuite(
		*logRate,
		*testDuration,
		*testType,
		*stdErr)

	if err != nil {
		log.Fatalf(err.Error())
	}

	testSuite, err := test.NewSuite(
		test.OptionSupervisorURL(*supervisorURL),
		test.OptionTesters(journaldSuite))

	if err != nil {
		log.Fatalf(err.Error())
	}

	testSuite.Start()
}
