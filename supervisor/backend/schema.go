package backend

import (
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/fatih/structs"
)

type DataProvider interface {
	Data() map[string]interface{}
}

// google store table schema
type BigQuerySchema struct {
	Name            string    `json:"name"`
	Timestamp       time.Time `json:"timestamp"`
	UserCPU_Usage   float64
	SystemCPU_Usage float64
	TotalCPU_Usage  float64
	Hostname        string `json:"hostname"`
	Instance        string `json:"instance"`
}

// Save implements the bigquery.ValueSaver interface.
func (b *BigQuerySchema) Save() (map[string]bigquery.Value, string, error) {
	bqRow := make(map[string]bigquery.Value)
	for k, v := range structs.Map(b) {
		bqRow[k] = v
	}
	return bqRow, "", nil
}

// Data
func (b *BigQuerySchema) Data() map[string]interface{} {
	return structs.Map(b)
}
