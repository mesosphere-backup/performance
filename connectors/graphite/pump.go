package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Sirupsen/logrus"
	"google.golang.org/api/iterator"
)

type Transformer interface {
	Graphite() ([]string, error)
	Time() time.Time
}

type SystemdMonitorEventsRow struct {
	Cluster_ID   string `json:"cluster_id"`
	Node_Type    string `json:"node_type"`
	Hostname    string `json:"hostname"`
	Systemd_Unit string `json:"systemd_unit"`
	User_CPU     float64 `json:"user_cpu"`
	Systemd_CPU  float64 `json:"systemd_cpu"`
	Total_CPU    float64 `json:"total_cpu"`
	Timestamp   time.Time `json:"timestamp"`
}

func (s *SystemdMonitorEventsRow) Graphite() ([]string, error) {
	nodeType := strings.Replace(s.Node_Type, ".", "_", -1)
	hostname := strings.Replace(s.Hostname, ".", "_", -1)

	lineUser := fmt.Sprintf("%s.%s.%s.systemd.%s.cpu.user %2f %d\n", s.Cluster_ID, nodeType, hostname, s.Systemd_Unit, s.User_CPU, s.Timestamp.Unix())
	lineSystem := fmt.Sprintf("%s.%s.%s.systemd.%s.cpu.system %2f %d\n", s.Cluster_ID, nodeType, hostname, s.Systemd_Unit, s.Systemd_CPU, s.Timestamp.Unix())
	lineTotal := fmt.Sprintf("%s.%s.%s.systemd.%s.cpu.total %2f %d\n", s.Cluster_ID, nodeType, hostname, s.Systemd_Unit, s.Total_CPU, s.Timestamp.Unix())

	return []string{lineUser, lineSystem, lineTotal}, nil
}

func (s *SystemdMonitorEventsRow) Time() time.Time {
	return s.Timestamp
}

type JournaldStressTestEventsRow struct {
	Cluster_ID   string `json:"cluster_id"`
	Node_Type    string `json:"node_type"`
	Hostname    string `json:"hostname"`
	Message_Rate float64 `json:"message_rate"`
	Units string `json:"units"`
	Timestamp   time.Time `json:"timestamp"`
}

func (j *JournaldStressTestEventsRow) Graphite() ([]string, error) {
	nodeType := strings.Replace(j.Node_Type, ".", "_", -1)
	hostname := strings.Replace(j.Hostname, ".", "_", -1)
	line := fmt.Sprintf("%s.%s.%s.%s %2f %d\n", j.Cluster_ID, nodeType, hostname, j.Units, j.Message_Rate, j.Timestamp.Unix())

	return []string{line}, nil
}

func (j *JournaldStressTestEventsRow) Time() time.Time {
	return j.Timestamp
}

func readRows(ctx context.Context, client *bigquery.Client, since time.Time, clusterID string) ([]Transformer, time.Time, error) {
	result := []Transformer{}

	bqConfig := bigquery.QueryConfig{
		DisableQueryCache: true,
	}

	query1 := fmt.Sprintf("SELECT * FROM [massive-bliss-781:dcos_performance.systemd_monitor_events] WHERE Timestamp > TIMESTAMP('%s') AND cluster_id = '%s' ORDER BY Timestamp", since.Format("2006-01-02 15:04:05 UTC"), clusterID)
	q := client.Query(query1)
	os.Stderr.WriteString(query1+"\n")
	bqConfig.Q = query1
	q.QueryConfig = bqConfig

	it, err := q.Read(ctx)
	if err != nil {
		return nil, since, err
	}

	for {
		m := &SystemdMonitorEventsRow{}
		err := it.Next(m)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, since, err
		}

		if m.Timestamp.UTC().After(since) {
			since = m.Timestamp.UTC()
		}

		result = append(result, m)
	}
	if len(result) > 0 {
		os.Stderr.WriteString(fmt.Sprintf("Fetched %d rows\n", len(result)))
	}
	return result, since, nil
}

func main() {
	since := time.Now().UTC()

	baseName := "CONNECTOR_"
	sinceStr := os.Getenv(baseName + "SINCE")
	clusterID := os.Getenv(baseName + "CLUSTER_ID")
	if clusterID == "" {
		panic(baseName+"CLUSTER_ID must be set")
	}

	if sinceStr != "" {
		dateCommandFormat := "Mon Jan  2 15:04:05 UTC 2006"
		var err error
		since, err = time.Parse(dateCommandFormat, sinceStr)
		if err != nil {
			panic(err)
		}
		since = since.UTC()
	}

	ctx := context.Background()

	projectID := "massive-bliss-781"

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		panic(err)
	}

	for {
		time.Sleep(time.Second * 2)

		metrics, t, err := readRows(ctx, client, since, clusterID)
		if err != nil {
			logrus.Errorf("Error fetching metrics: %s", err)
			continue
		}

		if len(metrics) > 0 {
			since = t
			for _, m := range metrics {
				lines, err := m.Graphite()
				if err != nil {
					logrus.Error(err)
					continue
				}
				for _, line := range lines {
					fmt.Printf("metrics."+line)
				}
			}
		}
	}
}