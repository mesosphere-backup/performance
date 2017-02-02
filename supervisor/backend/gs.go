package backend

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/bigquery"
)

// Backend describes the storage interface to store results from experiment.
type Backend interface {
	// ID returnes storage unique ID.
	ID() string

	// Put item to a storage.
	Put(context.Context, interface{}) error

	// CreateTable must create a table for data.
	CreateTable(ctx context.Context) error
}

// NewFlatBigQuery returns a new instance of FlatBigQuery. A backend type to upload results
// to google BigQuery.
func NewFlatBigQuery(ctx context.Context, projectID, dataset, tableName string) (*FlatBigQuery, error) {
	if projectID == "" || dataset == "" || tableName == "" {
		return nil, errors.New("projectID, dataset and tableName cannot be empty")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return &FlatBigQuery{
		ProjectID: projectID,
		Dataset:   dataset,
		TableName: tableName,

		client:   bqClient,
		uploader: bqClient.Dataset(dataset).Table(tableName).Uploader(),
	}, nil
}

// FlatBigQuery is a big query implementation of Backend.
type FlatBigQuery struct {
	ProjectID string
	Dataset   string
	TableName string

	client   *bigquery.Client
	uploader *bigquery.Uploader
}

// ID returns a backend name.
func (f *FlatBigQuery) ID() string {
	return fmt.Sprintf("Flat BigQuery. ProjectID: %s, Dataset: %s, TableName: %s", f.ProjectID, f.Dataset, f.TableName)
}

// Put uploads item to big query.
func (t *FlatBigQuery) Put(ctx context.Context, item interface{}) error {
	if ctx == nil {
		ctx = context.Background()
	}

	row, ok := item.(*BigQuerySchema)
	if ok {
		rows := []*BigQuerySchema{row}
		return t.uploader.Put(ctx, rows)
	}

	bqRows := []*BigQuerySchema{}

	rows, ok := item.([]map[string]interface{})
	if ok {
		for _, row := range rows {
			bqSchema := &BigQuerySchema{}
			s := reflect.ValueOf(bqSchema).Elem()
			typeOfT := s.Type()

			updatedFieldsNum := 0
			for i := 0; i < s.NumField(); i++ {
				f := s.Field(i)
				if v, ok := row[typeOfT.Field(i).Name]; ok {
					switch f.Kind() {
					case reflect.String:
						f.SetString(v.(string))
						updatedFieldsNum++
					case reflect.Float64:
						f.SetFloat(v.(float64))
						updatedFieldsNum++
					case reflect.Struct:

						switch f.Type() {
						case reflect.TypeOf(time.Time{}):
							f.Set(reflect.ValueOf(v))
						}
						updatedFieldsNum++
					}
				}
			}
			if updatedFieldsNum > 0 {
				bqRows = append(bqRows, bqSchema)
			}
		}
		return t.uploader.Put(ctx, bqRows)
	}
	return fmt.Errorf("Unable to upload item %+v to %s", item, t.ID())
}

// CreateTable create a new big query table.
func (t *FlatBigQuery) CreateTable(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	schema, err := bigquery.InferSchema(BigQuerySchema{})
	if err != nil {
		return err
	}

	return t.client.Dataset(t.Dataset).Table(t.TableName).Create(ctx, schema)
}
