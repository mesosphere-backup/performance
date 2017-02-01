package backend

import (
	"time"

	"github.com/fatih/structs"
)

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

func (b *BigQuerySchema) ToBigQueryRow() *BigQueryRow {
	row := NewBigQueryRow()
	for key, value := range structs.Map(b) {
		row.Data[key] = value
	}
	return row
}
