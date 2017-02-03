package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/mesosphere/performance/supervisor/backend"
)

func PostToSupervisor(url string, event backend.BigQuerySchema) error {
	fmt.Println("Sending to %s", url)
	json, err := json.Marshal(event)
	if err != nil {
		return err
	}

	client := http.Client{}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(json))
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("Got %d response code from supervisor", resp.StatusCode))
	}

	return nil
}
