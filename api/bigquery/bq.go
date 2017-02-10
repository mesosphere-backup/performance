package bigquery

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"
)

func NewBigQuery(ctx context.Context, projectID string) (*BigQuery, error) {
	if projectID == "" {
		return nil, errors.New("projectID cannot be empty")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return &BigQuery{
		ProjectID: projectID,

		Client: bqClient,
	}, nil
}

// BigQuery is a single table uploader
type BigQuery struct {
	ProjectID string

	Client   *bigquery.Client
}

type EventData map[string]bigquery.Value

func (e EventData) Save() (map[string]bigquery.Value, string, error) {
	return e, "", nil
}

// Event is a structure describes a user sent event.
type Event struct {
	// event control fields
	UploadTimeout string            `json:"upload_timeout"`
	SendImmediately bool            `json:"send_immediately"`

	// bigquery params
	Table string                   `json:"table"`

	// event data
	NodeType string                `json:"node_type"`
	Hostname string                `json:"hostname"`
	Data []*EventData              `json:"data"`
}

func (e *Event) Validate() error {
	// TODO: ready the required fields from a tag
	if e.Table == "" || len(e.Data) == 0 || e.NodeType == "" || e.Hostname == "" {
		return errors.New("Invalid event headers")
	}

	return nil
}
