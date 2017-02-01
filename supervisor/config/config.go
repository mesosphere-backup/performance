package config

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
)

const supervisor = "supervisor"

type Config struct {
	FlagVerbose            bool
	FlagWebServerBind      string
	FlagWaitBetweenCollect string
	FlagCPUUsageInterval   string
	FlagUploadInterval     string

	// bigquery config
	FlagProjectID  string
	FlagDataSet    string
	FlagTableName  string
	FlagBufferSize int

	// unexported values
	Wait             time.Duration
	CPUUsageInterval time.Duration
	UploadInterval   time.Duration
}

func (c *Config) setFlags(fs *flag.FlagSet) {
	fs.BoolVar(&c.FlagVerbose, "verbose", c.FlagVerbose, "Print out verbose output.")
	fs.StringVar(&c.FlagWebServerBind, "bind", c.FlagWebServerBind, "Bind to addr:port.")
	fs.StringVar(&c.FlagWaitBetweenCollect, "interval", c.FlagWaitBetweenCollect, "Set metrics collection interval.")
	fs.StringVar(&c.FlagCPUUsageInterval, "cpu-interval", c.FlagCPUUsageInterval, "Set cpu usage report interval.")
	fs.StringVar(&c.FlagUploadInterval, "upload-interval", c.FlagUploadInterval, "Set upload interval.")
	fs.IntVar(&c.FlagBufferSize, "rows-buffer", c.FlagBufferSize, "Set rows buffer size.")

	fs.StringVar(&c.FlagProjectID, "project-id", c.FlagProjectID, "Set bigquery ProjectID.")
	fs.StringVar(&c.FlagDataSet, "dataset", c.FlagDataSet, "Set bigquery dataset.")
	fs.StringVar(&c.FlagTableName, "table", c.FlagTableName, "Set bigquery table name.")
}

func NewConfig(args []string) (c *Config, err error) {
	if len(args) == 0 {
		return nil, errors.New("arguments cannot be empty")
	}

	c = &Config{}

	// default values
	c.FlagWebServerBind = ":9123"
	c.FlagWaitBetweenCollect = "3s"
	c.FlagCPUUsageInterval = "2s"
	c.FlagUploadInterval = "10s"

	c.FlagProjectID = "massive-bliss-781"
	c.FlagDataSet = "dcos_performance2"
	c.FlagTableName = "mnaboka"
	c.FlagBufferSize = 1000

	flagSet := flag.NewFlagSet(supervisor, flag.ContinueOnError)
	c.setFlags(flagSet)

	if err := flagSet.Parse(args[1:]); err != nil {
		return nil, err
	}

	if c.FlagVerbose {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("Using debug level")
	}

	c.Wait, err = time.ParseDuration(c.FlagWaitBetweenCollect)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse flag interval: %s", err)
	}

	c.CPUUsageInterval, err = time.ParseDuration(c.FlagCPUUsageInterval)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse flag cpu-interval: %s", err)
	}

	c.UploadInterval, err = time.ParseDuration(c.FlagUploadInterval)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse flag upload-interval: %s", err)
	}

	if c.Wait <= 0 || c.CPUUsageInterval <= 0 || c.UploadInterval <= 0 {
		return nil, fmt.Errorf("Invalid Wait interval %s", c.Wait.String())
	}

	return c, nil
}
