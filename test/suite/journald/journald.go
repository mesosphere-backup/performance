package journald

import (
	"context"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mesosphere/performance/supervisor/backend"
)

/*
Use a basic naming convention for Name parameter which is
used as event name:
	TEST_SUITE::TEST_NAME::ACTION::VALUE::UNIT
*/
const (
	TEST_SUITE       = "journald"
	START            = "start"
	STOP             = "stop"
	LINES_PER_SECOND = "lines/second"
	DELIMITER        = "::"
)

var jlog = logrus.WithFields(logrus.Fields{
	"suite": "journald",
})

// JournaldTestSuite object implements scale.Tester for journald scale testing
type JournaldTestSuite struct {
	LoggingRate  int
	StdErr       bool
	TestDuration int
	TestType     string
	EventChan    chan backend.BigQuerySchema
	Context      context.Context
}

// NewTestSuite returns a valid JournalTestSuite object that implements a scale.Tester
func NewTestSuite(logRate, testDuration int, testType string, stdErr bool) (JournaldTestSuite, error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(testDuration)*time.Second)

	return JournaldTestSuite{
		LoggingRate:  logRate,
		EventChan:    make(chan backend.BigQuerySchema),
		StdErr:       stdErr,
		TestDuration: testDuration,
		TestType:     testType,
		Context:      ctx,
	}, nil
}

func (j JournaldTestSuite) Run(supervisorURL string) error {
	if j.TestType == "constant-rate" {
		ConstantRate(supervisorURL, j)
	}

	return nil
}

func (j JournaldTestSuite) GetEvent() chan backend.BigQuerySchema {
	return j.EventChan
}

func (j JournaldTestSuite) GetContext() context.Context {
	return j.Context
}
