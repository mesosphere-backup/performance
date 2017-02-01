package backend

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
)

// Backend describes the storage interface to store results from experiment.
type Backend interface {
	// ID returnes storage unique ID.
	ID() string

	// Put item to a storage.
	Put(context.Context, interface{}) error
}

func NewBigQueryRow() *BigQueryRow {
	return &BigQueryRow{
		Data: map[string]bigquery.Value{},
	}
}

type BigQueryRow struct {
	Data map[string]bigquery.Value
}

func (b *BigQueryRow) Save() (map[string]bigquery.Value, string, error) {
	return b.Data, "", nil
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

// FlatBigQuery is a single table uploader
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

func (t *FlatBigQuery) Put(ctx context.Context, item interface{}) error {
	if ctx == nil {
		ctx = context.Background()
	}

	rows, ok := item.([]*BigQueryRow)
	if !ok {
		return errors.New("Item must be a list of references to BigQueryRow onject")
	}

	return t.uploader.Put(ctx, rows)
}

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
