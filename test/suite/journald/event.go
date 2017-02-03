package journald

import (
	"github.com/mesosphere/performance/supervisor/backend"
	"github.com/shirou/gopsutil/host"
)

func newEvent(name string) (backend.BigQuerySchema, error) {
	event := backend.BigQuerySchema{}

	hostname, err := getHostname()
	if err != nil {
		return event, err
	}
	event.Hostname = hostname

	taskID, err := getTaskID()
	if err != nil {
		return event, err
	}
	event.Instance = taskID

	event.Name = name

	return event, nil
}

func getTaskID() (string, error) {
	return "12345", nil
}

func getHostname() (string, error) {
	h, err := host.Info()
	if err != nil {
		return "", err
	}

	return h.Hostname, nil
}
