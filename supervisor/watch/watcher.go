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
	"github.com/mesosphere/journald-scale-test/supervisor/proc"
	"github.com/mesosphere/journald-scale-test/supervisor/systemd"
)

type Event map[string]interface{}

// StartWatcher starts watching host systemd units
func StartWatcher(ctx context.Context, backends []backend.Backend, eventChan <-chan *backend.BigQuerySchema) {
	if ctx == nil {
		ctx = context.Background()
	}

	resultChan := make(chan *SystemdUnitStatus)

	go processResult(ctx, resultChan, backends, eventChan)

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Stop monitoring")
			return
		default:
		}

		logrus.Println("Start monitoring")
		units, err := systemd.GetSystemdUnitsProps()
		if err != nil {
			logrus.Errorf("Unable to get a list of systemd units: %s", err)
			continue
		}

		wg := &sync.WaitGroup{}

		for _, unit := range units {
			wg.Add(1)
			go handleUnit(unit, wg, resultChan)
		}

		logrus.Info("Waiting for results")
		wg.Wait()
		logrus.Info("Done")
	}
}

// SystemdUnitStatus a structure that holds systemd unit name, pid and cpu utilization by it.
type SystemdUnitStatus struct {
	Name     string
	Pid      uint32
	CPUUsage *proc.CPUPidUsage
}

func handleUnit(unit *systemd.SystemdUnitProps, wg *sync.WaitGroup, resultChan chan<- *SystemdUnitStatus) {
	defer wg.Done()

	// TODO: move to config
	usage, err := proc.LoadByPID(int32(unit.Pid), time.Second*2)
	if err != nil {
		logrus.Errorf("Unit %s. Error %s", unit.Name, err)
		return
	}

	resultChan <- &SystemdUnitStatus{
		Name:     unit.Name,
		Pid:      unit.Pid,
		CPUUsage: usage,
	}
}

func processResult(ctx context.Context, results <-chan *SystemdUnitStatus, backends []backend.Backend, eventChan <-chan *backend.BigQuerySchema) {
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

			//TODO: move to config and add to drain after certain time elapsed
			if len(rows) > 1000 || time.Since(updateTime) > time.Second * 10 {
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
