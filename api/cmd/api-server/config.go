package main

import (
	"errors"
	"flag"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
)

const server = "api-server"

// Config
type Config struct {
	FlagVerbose bool

	FlagProjectID            string
	FlagDataset              string
	FlagEventStreamTableName string
	FlagEventBuffer          int
	FlagEventFlushInterval   string
	FlagClusterID string

	EventUploadInterval      time.Duration
}

func (c *Config) setFlags(fs *flag.FlagSet) {
	fs.BoolVar(&c.FlagVerbose, "verbose", c.FlagVerbose, "Print out verbose output.")
	fs.StringVar(&c.FlagProjectID, "project-id", c.FlagProjectID, "Set BigQuery project ID.")
	fs.StringVar(&c.FlagDataset, "dataset", c.FlagDataset, "Set BigQuery dataset.")
	fs.StringVar(&c.FlagEventStreamTableName, "event-stream-table", c.FlagEventStreamTableName, "Set event stream table name.")
	fs.IntVar(&c.FlagEventBuffer, "event-buffer-size", c.FlagEventBuffer, "Set buffer size for events.")
	fs.StringVar(&c.FlagEventFlushInterval, "flush-buffer", c.FlagEventFlushInterval, "Set upload to bigquery interval.")
	fs.StringVar(&c.FlagClusterID, "cluster-id", c.FlagClusterID, "Set cluster ID.")
}

func NewConfig(args []string) (*Config, error) {
	if len(args) == 0 {
		return nil, errors.New("arguments cannot be empty")
	}

	c := &Config{
		FlagProjectID: "massive-bliss-781",
		FlagDataset: "dcos_performance",
		FlagEventStreamTableName: "event_stream",
		FlagEventBuffer: 500,
		FlagEventFlushInterval: "10s",
	}

	envPrefix := "API_SERVER_"
	if v := os.Getenv(envPrefix+"CLUSTER_ID"); v != "" {
		c.FlagClusterID = v
	}

	if v := os.Getenv(envPrefix+"PROJECT_ID"); v != "" {
		c.FlagProjectID = v
	}

	if v := os.Getenv(envPrefix+"DATASET"); v != "" {
		c.FlagDataset = v
	}

	if v := os.Getenv(envPrefix+"FLUSH_INTERVAL"); v != "" {
		c.FlagEventFlushInterval = v
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

	var err error
	c.EventUploadInterval, err = time.ParseDuration(c.FlagEventFlushInterval)
	if err != nil {
		return nil, err
	}

	if c.FlagClusterID == "" {
		return nil, errors.New("-cluster-config is required.")
	}

	return c, nil
}
