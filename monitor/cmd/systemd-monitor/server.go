package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mesosphere/performance/api/bigquery"
	"github.com/mesosphere/performance/monitor/proc"
	"github.com/mesosphere/performance/monitor/systemd"
)

// SystemdUnitStatus a structure that holds systemd unit name, pid and cpu utilization by it.
type SystemdUnitStatus struct {
	Name     string
	Pid      uint32
	CPUUsage *proc.CPUPidUsage
}

func StartWatcher(ctx context.Context, cfg *Config, cancel context.CancelFunc) {
	client := &http.Client{
		Transport: http.DefaultTransport,
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				logrus.Info("Shutting down watcher")

			case <-time.After(cfg.interval):
				logrus.Info("Start processUnits")
				if err := processUnits(ctx, client, cfg, hostname); err != nil {
					logrus.Error(err)
				}
			}
		}
	}()
	<-ctx.Done()
}

func processUnits(ctx context.Context, client *http.Client, cfg *Config, hostname string) error {
	units, err := systemd.GetSystemdUnitsProps()
	if err != nil {
		return fmt.Errorf("Unable to get a list of systemd units: %s", err)
	}

	resultChan := make(chan *SystemdUnitStatus, len(units))

	wg := &sync.WaitGroup{}
	for _, unit := range units {
		wg.Add(1)
		go handleUnit(ctx, unit, cfg, wg, resultChan)
	}

	wg.Wait()
	return processResult(ctx, client, cfg, resultChan, hostname)
}

func handleUnit(ctx context.Context, unit *systemd.SystemdUnitProps, cfg *Config, wg *sync.WaitGroup, resultChan chan<- *SystemdUnitStatus) {
	defer wg.Done()

	usage, err := proc.LoadByPID(int32(unit.Pid), cfg.cpuWait)
	if err != nil {
		logrus.Errorf("Unit %s. Error %s", unit.Name, err)
		return
	}

	select {
	case <-ctx.Done():
		return
	default:
		resultChan <- &SystemdUnitStatus{
			Name:     unit.Name,
			Pid:      unit.Pid,
			CPUUsage: usage,
		}
	}
}

func processResult(ctx context.Context, client *http.Client, cfg *Config, results <-chan *SystemdUnitStatus, hostname string) error {
	var events []*SystemdUnitStatus

	for {
		select {
		case <-ctx.Done():
			logrus.Infof("Shutting down result processor")
			return nil

		case event := <-results:
			events = append(events, event)
		default:
			logrus.Infof("Collected %d events. Posting to %s", len(events), cfg.FlagPostURL)
			return postResult(client, cfg, events, hostname)
		}
	}
}

func postResult(client *http.Client, cfg *Config, unitsStatus []*SystemdUnitStatus, hostname string) error {
	event := &bigquery.Event{
		UploadTimeout: "5s",
		Table: "systemd_monitor_data",
		ClusterID: cfg.FlagClusterID,
		NodeType: cfg.FlagRole,
		Hostname: hostname,
	}

	for _, unitStatus := range unitsStatus {
		eventData := &bigquery.EventData{
			"systemd_unit": unitStatus.Name,
			"user_cpu":     unitStatus.CPUUsage.User,
			"system_cpu":   unitStatus.CPUUsage.System,
			"total_cpu":    unitStatus.CPUUsage.Total,
		}
		event.Data = append(event.Data, eventData)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	bodyReader := bytes.NewReader(body)
	req, err := http.NewRequest("POST", cfg.FlagPostURL, bodyReader)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.postTimeout)
	defer cancel()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s returned code %d", cfg.FlagPostURL, resp.StatusCode)
	}
	return nil
}
