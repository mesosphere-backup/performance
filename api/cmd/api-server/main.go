package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
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
	bq, err := bigquery.NewBigQuery(ctx, cfg.FlagProjectID)
	if err != nil {
		return nil, err
	}

	api := &APIServer{
		BigQuery: bq,
		Cfg: cfg,

		eventStreamUploader: bq.Client.Dataset(cfg.FlagDataset).Table(cfg.FlagEventStreamTableName).Uploader(),
	}

	go api.startEventTimer(ctx)

	return api, nil
}

type eventBuffer struct {
	EventStreamRow *bigquery.EventData

	EventRows       []*bigquery.EventData
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
			if len(a.eventsBuffer) >= a.Cfg.FlagEventBuffer {
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
			ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
		}

		if err := a.eventStreamUploader.Put(ctx, bufferedEvent.EventStreamRow); err != nil {
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
	timestamp := time.Now().UTC()

	if err := e.Validate(); err != nil {
		return err
	}

	var uploadTimeout time.Duration
	if e.UploadTimeout != "" {
		var err error
		uploadTimeout, err = time.ParseDuration(e.UploadTimeout)
		if err != nil {
			logrus.Error(err)
		}
	}

	eventID := uuid.New().String()

	eventStreamRow := &bigquery.EventData{
		"cluster_id": a.Cfg.FlagClusterID,
		"node_type": e.NodeType,
		"event_id": eventID,
		"hostname": e.Hostname,
		"timestamp": timestamp,
	}

	eventUploader := a.BigQuery.Client.Dataset(a.Cfg.FlagDataset).Table(e.Table).Uploader()
	for _, eventData := range e.Data {
		(*eventData)["event_id"] = eventID
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
			ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
		}
		defer cancel()

		if err := a.eventStreamUploader.Put(ctx, eventStreamRow); err != nil {
			return err
		}
		return eventUploader.Put(ctx, e.Data)
	}

	a.Lock()
	a.eventsBuffer = append(a.eventsBuffer, &eventBuffer{
		EventStreamRow: eventStreamRow,

		EventRows: e.Data,
		EventUploader: eventUploader,
		Timeout: uploadTimeout,
	})
	a.Unlock()

	return nil
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
	cfg, err := NewConfig(os.Args)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	if err := startServer(ctx, cfg); err != nil {
		panic(err)
	}
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	api := requestJobFromContext(r.Context())

	var event bigquery.Event
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&event); err != nil {
		http.Error(w, "Unable to unmarshal user input", http.StatusBadRequest)
		return
	}

	if err := event.Validate(); err != nil {
		http.Error(w, "Bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := api.SubmitEvent(&event); err != nil {
		logrus.Errorf("Unable to submit an event: %s", err)
		http.Error(w, "Internal server error: "+err.Error(), http.StatusInternalServerError)
		return
	}

}
