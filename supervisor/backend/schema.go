package backend

import (
	"time"

	"github.com/fatih/structs"
)

// google store table schema
type BigQuerySchema struct {
	Name            string
	Timestamp       time.Time
	UserCPU_Usage   float64
	SystemCPU_Usage float64
	TotalCPU_Usage  float64
	Hostname        string
	Instance        string
}

func (b *BigQuerySchema) ToBigQueryRow() *BigQueryRow {
	row := NewBigQueryRow()
	for key, value := range structs.Map(b) {
		row.Data[key] = value
	}
	return row
}
