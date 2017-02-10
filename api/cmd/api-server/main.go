package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	googleBigquery "cloud.google.com/go/bigquery"
	"github.com/Sirupsen/logrus"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/mesosphere/performance/api/bigquery"
)

type key int

const requestIDKey key = 0

func newContextWithJob(ctx context.Context, job *APIServer, req *http.Request) context.Context {
	return context.WithValue(ctx, requestIDKey, job)
}

func requestJobFromContext(ctx context.Context) *APIServer {
	return ctx.Value(requestIDKey).(*APIServer)
}

func middleware(next http.Handler, job *APIServer) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		ctx := newContextWithJob(req.Context(), job, req)
		next.ServeHTTP(rw, req.WithContext(ctx))
	})
}

func NewAPIServer(ctx context.Context, cfg *Config) (*APIServer, error) {
	bq, err := bigquery.NewBigQuery(ctx, cfg.ProjectID)
	if err != nil {
		return nil, err
	}

	api := &APIServer{
		BigQuery: bq,
		Cfg: cfg,

		eventStreamUploader: bq.Client.Dataset(cfg.Dataset).Table(cfg.EventStreamTableName).Uploader(),
	}

	go api.startEventTimer(ctx)

	return api, nil
}

type eventBuffer struct {
	EventStreamRows []bigquery.EventData

	EventRows       []bigquery.EventData
	EventUploader  *googleBigquery.Uploader
	Timeout        time.Duration
}

// API Server
type APIServer struct {
	sync.Mutex

	BigQuery *bigquery.BigQuery
	Cfg *Config

	eventStreamUploader *googleBigquery.Uploader
	eventsBuffer []*eventBuffer
	lastBulkEventsUploaded time.Time
}

func (a *APIServer) startEventTimer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-time.After(time.Second*5):
			// if we have enough events, flush everything
			if len(a.eventsBuffer) >= a.Cfg.EventBuffer {
				logrus.Infof("Buffer is full. Collected %d events. Uploading", len(a.eventsBuffer))
				err := a.FlushEventBuffer()
				if err != nil {
					logrus.Errorf("Unable to flush buffer: %s", err)
				}
			} else if time.Since(a.lastBulkEventsUploaded) > a.Cfg.EventUploadInterval && len(a.eventsBuffer) > 0 {
				logrus.Infof("Timeout reached. Collected %d events. Uploading", len(a.eventsBuffer))
				a.FlushEventBuffer()
				err := a.FlushEventBuffer()
				if err != nil {
					logrus.Errorf("Unable to flush buffer: %s", err)
				}
			}
		}
	}
}

func (a *APIServer) FlushEventBuffer() error {
	a.Lock()
	defer a.Unlock()

	var errs []string
	for _, bufferedEvent := range a.eventsBuffer {
		s := time.Now()
		var (
			ctx context.Context
			cancel context.CancelFunc
		)
		if bufferedEvent.Timeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), bufferedEvent.Timeout)
		} else {
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		}

		if err := a.eventStreamUploader.Put(ctx, bufferedEvent.EventStreamRows); err != nil {
			logrus.Error(err)
			errs = append(errs, err.Error())
		}

		if err := bufferedEvent.EventUploader.Put(ctx, bufferedEvent.EventRows); err != nil {
			logrus.Error(err)
			errs = append(errs, err.Error())
		}
		cancel()
		logrus.Infof("Processed entry in %s", time.Since(s).String())
	}

	a.eventsBuffer = []*eventBuffer{}
	a.lastBulkEventsUploaded = time.Now()

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ";"))
	}
	return nil
}

func (a *APIServer) SubmitEvent(e *bigquery.Event) error {
	if err := e.Validate(); err != nil {
		return err
	}


	timestamp := time.Now().UTC()

	var uploadTimeout time.Duration
	if e.UploadTimeout != "" {
		var err error
		uploadTimeout, err = time.ParseDuration(e.UploadTimeout)
		if err != nil {
			logrus.Error(err)
		}
	}

	var eventStreamRows []bigquery.EventData
	eventUploader := a.BigQuery.Client.Dataset(a.Cfg.Dataset).Table(e.Table).Uploader()
	for _, eventData := range e.Data {
		eventID := uuid.New().String()

		eventData["primary_key"] = eventID
		eventData["timestamp"] = timestamp

		eventStreamRows = append(eventStreamRows, bigquery.EventData{
			"cluster_id": e.ClusterID,
			"node_type": e.NodeType,
			"event_id": eventID,
			"hostname": e.Hostname,
		})
	}


	if e.SendImmediately {
		logrus.Info("Uploading an event immediately")
		var (
			ctx context.Context
			cancel context.CancelFunc
		)
		if uploadTimeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), uploadTimeout)
		} else {
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		}
		defer cancel()

		if err := a.eventStreamUploader.Put(ctx, eventStreamRows); err != nil {
			return err
		}
		return eventUploader.Put(ctx, e.Data)
	}

	a.Lock()
	a.eventsBuffer = append(a.eventsBuffer, &eventBuffer{
		EventStreamRows: eventStreamRows,

		EventRows: e.Data,
		EventUploader: eventUploader,
		Timeout: uploadTimeout,
	})
	a.Unlock()
	logrus.Infof("After insert")

	return nil
}

// Config
type Config struct {
	ProjectID string
	Dataset string
	EventStreamTableName string
	EventBuffer int
	EventUploadInterval time.Duration
}

func NewConfig() (*Config, error) {
	return &Config{
		ProjectID: "massive-bliss-781",
		Dataset: "dcos_performance",
		EventStreamTableName: "event_stream",
		EventBuffer: 500,
		EventUploadInterval: time.Second * 5,
	}, nil
}

func startServer(ctx context.Context, cfg *Config) error {
	api, err := NewAPIServer(ctx, cfg)
	if err != nil {
		return err
	}

	router := mux.NewRouter()
	router.Path("/events").Handler(middleware(http.HandlerFunc(eventsHandler), api)).Methods("POST")

	logrus.Infof("Started on :9123", )
	return http.ListenAndServe(":9123", router)
}

func main() {
	cfg, err := NewConfig()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	panic(startServer(ctx, cfg))
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	api := requestJobFromContext(r.Context())

	var event *bigquery.Event
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&event); err != nil {
		http.Error(w, "Unable to unmarshal user input", http.StatusBadRequest)
		return
	}

	if err := event.Validate(); err != nil {
		http.Error(w, "Bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := api.SubmitEvent(event); err != nil {
		logrus.Errorf("Unable to submit an event: %s", err)
		http.Error(w, "Internal server error: "+err.Error(), http.StatusInternalServerError)
		return
	}

}
