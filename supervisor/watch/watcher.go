package watch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mesosphere/journald-scale-test/supervisor/backend"
	"github.com/mesosphere/journald-scale-test/supervisor/config"
	"github.com/mesosphere/journald-scale-test/supervisor/proc"
	"github.com/mesosphere/journald-scale-test/supervisor/systemd"
)


// StartWatcher starts watching host systemd units
func StartWatcher(ctx context.Context, cfg *config.Config, backends []backend.Backend,
	eventChan <-chan proc.Reporter) {
	if ctx == nil {
		ctx = context.Background()
	}

	dataChan := make(chan []proc.Reporter)

	go processResult(ctx, cfg, dataChan, backends, eventChan)

	for {
		if err := watchMetrics(ctx, cfg, dataChan); err != nil {
			logrus.Error(err)
		}

		select {
		case <-ctx.Done():
			logrus.Info("Shutting down watcher")
			close(dataChan)

		case <-time.After(cfg.Wait):
		}
	}
}

func watchMetrics(ctx context.Context, cfg *config.Config, dataChan chan<- []proc.Reporter) error {
	units, err := systemd.GetSystemdUnitsProps()
	if err != nil {
		return fmt.Errorf("Unable to get a list of systemd units: %s", err)
	}

	wg := &sync.WaitGroup{}
	for _, unit := range units {
		wg.Add(1)
		go handleUnit(ctx, unit, cfg, wg, dataChan)
	}

	wg.Wait()
	return nil
}

func handleUnit(ctx context.Context, unit *systemd.SystemdUnitProps, cfg *config.Config, wg *sync.WaitGroup,
	        dataChan chan<- []proc.Reporter) {
	defer wg.Done()

	usage, err := proc.LoadByPID(int32(unit.Pid), cfg.CPUUsageInterval)
	if err != nil {
		logrus.Errorf("Unit %s. Error %s", unit.Name, err)
		return
	}

	data := []proc.Reporter{usage, unit}

	select {
	case <-ctx.Done():
		return
	default:
		dataChan<- data
	}
}

func processResult(ctx context.Context, cfg *config.Config, dataChan <-chan []proc.Reporter,
	           backends []backend.Backend, eventChan <-chan proc.Reporter) {

	rows := make([]map[string]interface{}, 0)
	updateTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			logrus.Infof("Shutting down row processor")
			return

		case event := <-eventChan:
			if err := upload(ctx, event, backends); err != nil {
				logrus.Errorf("Error saving a new event: %s", err)
			}

		case data := <-dataChan:
			row := make(map[string]interface{})
			for _, piece := range data {
				for k, v := range piece.Data() {
					row[k] = v
				}
			}

			rows = append(rows, row)
			if len(rows) >= cfg.FlagBufferSize || time.Since(updateTime) >= cfg.UploadInterval {
				if err := upload(ctx, rows, backends); err != nil {
					logrus.Error(err)
					continue
				}
				rows = make([]map[string]interface{}, 0)
				updateTime = time.Now()
			}
		}
	}
}

func upload(ctx context.Context, items interface{}, backends []backend.Backend) error {
	for _, b := range backends {
		if err := b.Put(ctx, items); err != nil {
			return fmt.Errorf("Error uploading to backend %s: %s", b.ID(), err)
		}
		logrus.Infof("Uploaded to storage %s", b.ID())
	}
	return nil
}
