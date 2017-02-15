package test

import (
	"context"

	"github.com/Sirupsen/logrus"
	"github.com/mesosphere/performance/api/bigquery"
)

var slog = logrus.WithFields(logrus.Fields{
	"suite": "scale-suite",
})

// ScaleTester represents a generic scale test suite
type ScaleTester interface {
	Run(string) error
	GetEvent() chan bigquery.Event
	GetContext() context.Context
}

// ScaleTest type is a top level object abstracting the scale testing suite.
type ScaleTest struct {
	SupervisorURL string
	Testers       []ScaleTester
}

// NewTestSuite returns a valid SCaleTest object configured with functional
// parameter options.
func NewSuite(options ...TestOption) (ScaleTest, error) {
	scaleTestOptions := ScaleTest{}
	for _, option := range options {
		slog.Debugf("Adding test suite option %+v", option)
		if err := option(&scaleTestOptions); err != nil {
			return scaleTestOptions, err
		}
	}
	return scaleTestOptions, nil
}

// ScaleTest.Start executes the test suite for all testers{}
func (s ScaleTest) Start() error {
	slog.Info("Starting DC/OS scale test suite...")
	for _, test := range s.Testers {
		if err := test.Run(s.SupervisorURL); err != nil {
			slog.Error(err)
			continue
		}
	}

	return nil
}
