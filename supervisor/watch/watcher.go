package watch

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mesosphere/journald-scale-test/supervisor/backend"
	"github.com/mesosphere/journald-scale-test/supervisor/config"
	"github.com/mesosphere/journald-scale-test/supervisor/proc"
	"github.com/mesosphere/journald-scale-test/supervisor/systemd"
)

type Event map[string]interface{}

// StartWatcher starts watching host systemd units
func StartWatcher(ctx context.Context, cfg *config.Config, buffer int, uploadInterval time.Duration,
	          backends []backend.Backend, eventChan <-chan *backend.BigQuerySchema) error {
	if ctx == nil {
		ctx = context.Background()
	}

	resultChan := make(chan *SystemdUnitStatus)

	go processResult(ctx, buffer, uploadInterval, resultChan, backends, eventChan)

	for {
		if err := processUnits(ctx, cfg, resultChan); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			logrus.Info("Shutting down watcher")
			close(resultChan)
			return nil

		case <-time.After(cfg.Wait):
		}
	}
}

func processUnits(ctx context.Context, cfg *config.Config, resultChan chan<- *SystemdUnitStatus) error {
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

// SystemdUnitStatus a structure that holds systemd unit name, pid and cpu utilization by it.
type SystemdUnitStatus struct {
	Name     string
	Pid      uint32
	CPUUsage *proc.CPUPidUsage
}

func handleUnit(ctx context.Context, unit *systemd.SystemdUnitProps, cfg *config.Config, wg *sync.WaitGroup, resultChan chan<- *SystemdUnitStatus) {
	defer wg.Done()

	usage, err := proc.LoadByPID(int32(unit.Pid), cfg.CPUUsageInterval)
	if err != nil {
		logrus.Errorf("Unit %s. Error %s", unit.Name, err)
		return
	}

	select {
	case <- ctx.Done():
		return
	default:
		resultChan <- &SystemdUnitStatus{
			Name:     unit.Name,
			Pid:      unit.Pid,
			CPUUsage: usage,
		}
	}
}

func processResult(ctx context.Context, bufferSize int, uploadInterval time.Duration, results <-chan *SystemdUnitStatus,
	           backends []backend.Backend, eventChan <-chan *backend.BigQuerySchema) {
	rows := []*backend.BigQueryRow{}
	updateTime := time.Now()
	hostname, err := os.Hostname()
	if err != nil {
		logrus.Errorf("Unable to determine hostname: %s", err)
		hostname = "<undefined>"
	}

	for {
		select {
		case <-ctx.Done():
			logrus.Infof("Shutting down result processor")
			return

		case event := <-eventChan:
			if err := upload(ctx, []*backend.BigQueryRow{event.ToBigQueryRow()}, backends); err != nil {
				logrus.Errorf("Error saving a new event: %s", err)
			}

		case result := <-results:
			logrus.Debugf("[%s]: User %f; System %f; Total %f", result.Name,
				result.CPUUsage.User, result.CPUUsage.System, result.CPUUsage.Total)

			row := backend.BigQuerySchema{
				Name: result.Name,
				Timestamp: time.Now(),
				UserCPU_Usage: result.CPUUsage.User,
				SystemCPU_Usage: result.CPUUsage.User,
				TotalCPU_Usage: result.CPUUsage.Total,
				Hostname: hostname,
				Instance: strconv.Itoa(int(result.Pid)),
			}

			rows = append(rows, row.ToBigQueryRow())
			if len(rows) >= bufferSize || time.Since(updateTime) >= uploadInterval {
				if err := upload(ctx, rows, backends); err != nil {
					logrus.Error(err)
					continue
				}
				rows = []*backend.BigQueryRow{}
				updateTime = time.Now()
			}
		}
	}
}

func upload(ctx context.Context, items interface{}, backends []backend.Backend) error {
	for _, b := range backends {
		logrus.Infof("Uploading to storage %s", b.ID())
		if err := b.Put(ctx, items); err != nil {
			return fmt.Errorf("Error uploading to backend %s: %s", b.ID(), err)
		}
	}
	return nil
}
