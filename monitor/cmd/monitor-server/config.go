package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
)

const server = "api-server"

type Config struct {
	FlagVerbose      bool
	FlagPollInterval string
	FlagPostURL string

	FlagClusterID string
	FlagRole string

	interval time.Duration
	cpuWait time.Duration
}

func (c *Config) setFlags(fs *flag.FlagSet) {
	fs.BoolVar(&c.FlagVerbose, "verbose", c.FlagVerbose, "Print out verbose output.")
	fs.StringVar(&c.FlagPollInterval, "interval", c.FlagPollInterval, "Set metrics collection interval.")
	fs.StringVar(&c.FlagPostURL, "post-url", c.FlagPostURL, "Set API URL.")
	fs.StringVar(&c.FlagClusterID, "cluster-id", c.FlagClusterID, "Set cluster ID.")
	fs.StringVar(&c.FlagRole, "role", c.FlagRole, "Set role.")
}

func NewConfig(args []string) (c *Config, err error) {
	if len(args) == 0 {
		return nil, errors.New("arguments cannot be empty")
	}

	c = &Config{
		cpuWait: time.Second * 2,
	}

	// default values
	c.FlagPollInterval = "3s"
	c.FlagPostURL = "http://127.0.0.1:9123/incoming"

	flagSet := flag.NewFlagSet(server, flag.ContinueOnError)
	c.setFlags(flagSet)

	if err := flagSet.Parse(args[1:]); err != nil {
		return nil, err
	}

	if c.FlagVerbose {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("Using debug level")
	}

	i, err := time.ParseDuration(c.FlagPollInterval)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse flag interval: %s", err)
	}

	c.interval = i - c.cpuWait
	if c.interval < 2 {
		return nil, fmt.Errorf("Interval time must be >= %s", c.cpuWait + time.Second)
	}

	if c.FlagRole == "" || c.FlagClusterID == "" {
		return nil, errors.New("-role and -cluster-id are required")
	}

	return c, nil
}
