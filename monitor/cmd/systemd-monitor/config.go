package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"os"
)

const server = "api-server"

type Config struct {
	FlagVerbose      bool
	FlagPollInterval string
	FlagPostURL string
	FlagPostTimeout string

	FlagRole string

	postTimeout time.Duration
	interval time.Duration
	cpuWait time.Duration
}

func (c *Config) setFlags(fs *flag.FlagSet) {
	fs.BoolVar(&c.FlagVerbose, "verbose", c.FlagVerbose, "Print out verbose output.")
	fs.StringVar(&c.FlagPollInterval, "interval", c.FlagPollInterval, "Set metrics collection interval.")
	fs.StringVar(&c.FlagPostURL, "post-url", c.FlagPostURL, "Set API URL.")
	fs.StringVar(&c.FlagRole, "role", c.FlagRole, "Set role.")
	fs.StringVar(&c.FlagPostTimeout, "post-timeout", c.FlagPostTimeout, "Set timeout to execute a POST request.")
}

func NewConfig(args []string) (c *Config, err error) {
	if len(args) == 0 {
		return nil, errors.New("arguments cannot be empty")
	}

	c = &Config{
		cpuWait: time.Second * 2,
		FlagPollInterval: "3s",
		FlagPostTimeout: "1s",
		FlagPostURL: "http://127.0.0.1:9123/events",
	}

	envPrefix := "SYSTEMD_MONITOR_"
	if v := os.Getenv(envPrefix+"ROLE"); v != "" {
		c.FlagRole = v
	}

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

	c.postTimeout, err = time.ParseDuration(c.FlagPostTimeout)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse post-timeout: %s", err)
	}

	if c.FlagRole == "" {
		return nil, errors.New("-role is required")
	}

	return c, nil
}
