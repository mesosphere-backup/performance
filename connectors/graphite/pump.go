package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Sirupsen/logrus"
	"google.golang.org/api/iterator"
	"strings"
)

type Row struct {
	Cluster_ID   string `json:"cluster_id"`
	Node_Type    string `json:"node_type"`
	Hostname    string `json:"hostname"`
	Systemd_Unit string `json:"systemd_unit"`
	User_CPU     float64 `json:"user_cpu"`
	Systemd_CPU  float64 `json:"systemd_cpu"`
	Total_CPU    float64 `json:"total_cpu"`
	Timestamp   time.Time `json:"timestamp"`
}

func readRows(ctx context.Context, client *bigquery.Client, since time.Time, clusterID string) ([]*Row, error) {
	query := fmt.Sprintf("SELECT * FROM [massive-bliss-781:dcos_performance.systemd_monitor_events] WHERE Timestamp > TIMESTAMP('%s') AND cluster_id = '%s' ORDER BY Timestamp", since.Format("2006-01-02 15:04:05 UTC"), clusterID)
	os.Stderr.WriteString(query + "\n")
	q := client.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	result := []*Row{}
	for {
		m := &Row{}
		err := it.Next(m)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	os.Stderr.WriteString(fmt.Sprintf("Fetched %d rows\n", len(result)))
	return result, nil
}

func main() {
	since := time.Now().UTC()

	args := os.Args
	if len(args) < 2 {
		logrus.Fatalf("Usage: %s [cluster-id] [timestamp]", os.Args[0])
	}

	if len(args) == 3 {
		dateCommandFormat := "Mon Jan  2 15:04:05 UTC 2006"
		var err error
		since, err = time.Parse(dateCommandFormat, args[2])
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
		metrics, err := readRows(ctx, client, since, os.Args[1])
		if err != nil {
			panic(err)
		}

		if len(metrics) > 0 {
			for _, m := range metrics {
				hostname := strings.Replace(m.Hostname, ".", "_", -1)
				systemdUnit := strings.Replace(m.Systemd_Unit, ".", "_", -1)
				fmt.Printf("metrics.%s.%s.%s.cpu.total %.2f %d\n", m.Node_Type, hostname, systemdUnit, m.Total_CPU, m.Timestamp.Unix())


			}
			since = metrics[len(metrics)-1].Timestamp.UTC()
		}
		time.Sleep(time.Second * 2)
	}
}