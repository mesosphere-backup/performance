package journald

import (
	"github.com/mesosphere/performance/api/bigquery"
	"github.com/shirou/gopsutil/host"
)

func newEvent(name string) (bigquery.Event, error) {
	event := bigquery.Event{
		SendImmediately: true,
		Table: "TABLE_NAME_HERE",
		NodeType: "ROLE_HERE",
	}

	hostname, err := getHostname()
	if err != nil {
		return event, err
	}
	event.Hostname = hostname

	taskID, err := getTaskID()
	if err != nil {
		return event, err
	}

	data := &bigquery.EventData{
		"task_id": taskID,
	}
	event.Data = []*bigquery.EventData{data}

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
