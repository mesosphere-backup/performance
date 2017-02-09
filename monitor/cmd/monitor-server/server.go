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
	resultChan := make(chan *SystemdUnitStatus)

	client := &http.Client{
		Transport: http.DefaultTransport,
	}

	go processResult(ctx, client, cfg, resultChan, cancel)
	go func() {
		for {
			select {
			case <-ctx.Done():
				logrus.Info("Shutting down watcher")
				close(resultChan)

			case <-time.After(cfg.interval):
				if err := processUnits(ctx, cfg, resultChan); err != nil {
					logrus.Error(err)
				}
			}
		}
	}()
	<-ctx.Done()
}

func processUnits(ctx context.Context, cfg *Config, resultChan chan<- *SystemdUnitStatus) error {
	units, err := systemd.GetSystemdUnitsProps()
	if err != nil {
		return fmt.Errorf("Unable to get a list of systemd units: %s", err)
	}

	wg := &sync.WaitGroup{}
	for _, unit := range units {
		wg.Add(1)
		go handleUnit(ctx, unit, cfg, wg, resultChan)
	}

	wg.Wait()
	return nil
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

func processResult(ctx context.Context, client *http.Client, cfg *Config, results <-chan *SystemdUnitStatus, cancel context.CancelFunc) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	for {
		select {
		case <-ctx.Done():
			logrus.Infof("Shutting down result processor")
			return

		case result := <-results:
			logrus.Infof("Received result: %+v", result)
			if err := postResult(client, cfg, result, hostname); err != nil {
				logrus.Error(err)
				cancel()
			}
		}
	}
}

func postResult(client *http.Client, cfg *Config, result *SystemdUnitStatus, hostname string) error {

	data := bigquery.EventData{
		"systemd_unit": result.Name,
		"interval": cfg.interval.Seconds(),
		"user_cpu": result.CPUUsage.User,
		"system_cpu": result.CPUUsage.System,
		"total_cpu": result.CPUUsage.Total,
	}

	event := bigquery.Event{
		UploadTimeout: "2s",
		Table: "systemd_monitor_data",
		ClusterID: cfg.FlagClusterID,
		NodeType: cfg.FlagRole,
		Hostname: hostname,
		Data: data,
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

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s returned code %d", cfg.FlagPostURL, resp.StatusCode)
	}
	return nil
}
