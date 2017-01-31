package watch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mesosphere/journald-scale-test/supervisor/backend"
	"github.com/mesosphere/journald-scale-test/supervisor/proc"
	"github.com/mesosphere/journald-scale-test/supervisor/systemd"
)

// StartWatcher starts watching host systemd units
func StartWatcher(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	// TODO: move to config
	bq, err := backend.NewFlatBigQuery(ctx, "massive-bliss-781", "dcos_performance2", "mnaboka")
	if err != nil {
		panic("Unable to initialze big query backend: " + err.Error())
	}

	resultChan := make(chan *SystemdUnitStatus)

	// TODO: move to constructor
	backends := []backend.Backend{bq}
	go processResult(ctx, resultChan, backends)

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

func processResult(ctx context.Context, results <-chan *SystemdUnitStatus, backends []backend.Backend) {
	rows := []*backend.BigQueryRow{}
	for {
		select {
		case <-ctx.Done():
			logrus.Infof("Shutting down result processor")
			return
		case result := <-results:
			logrus.Debugf("[%s]: User %f; System %f; Total %f", result.Name,
				result.CPUUsage.User, result.CPUUsage.System, result.CPUUsage.Total)

			row := backend.NewBigQueryRow()
			row.Data["Name"] = result.Name
			row.Data["Timestamp"] = time.Now()
			row.Data["UserCPU_Usage"] = result.CPUUsage.User
			row.Data["SystemCPU_Usage"] = result.CPUUsage.User
			row.Data["TotalCPU_Usage"] = result.CPUUsage.Total

			rows = append(rows, row)

			//TODO: move to config and add to drain after certain time elapsed
			if len(rows) > 100 {
				if err := upload(ctx, rows, backends); err != nil {
					logrus.Error(err)
					continue
				}
				rows = []*backend.BigQueryRow{}
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
