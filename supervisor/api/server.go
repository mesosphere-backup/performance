package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/mesosphere/journald-scale-test/supervisor/backend"
	"github.com/mesosphere/journald-scale-test/supervisor/config"
	"github.com/mesosphere/journald-scale-test/supervisor/watch"
)

type key int

const requestIDKey key = 0

type Job struct {
	sync.Mutex

	cancel   context.CancelFunc
	backends []backend.Backend
	events   chan *backend.BigQuerySchema
}

func newContextWithJob(ctx context.Context, job *Job, req *http.Request) context.Context {
	return context.WithValue(ctx, requestIDKey, job)
}

func requestJobFromContext(ctx context.Context) *Job {
	return ctx.Value(requestIDKey).(*Job)
}

func middleware(next http.Handler, job *Job) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		ctx := newContextWithJob(req.Context(), job, req)
		next.ServeHTTP(rw, req.WithContext(ctx))
	})
}

// StartWebServer starts a gorilla mux web server.
func StartWebServer(cfg *config.Config) error {
	ctx, cancel := context.WithCancel(context.Background())

	bq, err := backend.NewFlatBigQuery(ctx, cfg.FlagProjectID, cfg.FlagDataSet, cfg.FlagTableName)
	if err != nil {
		return err
	}

	job := &Job{
		cancel: cancel,
		backends: []backend.Backend{bq},
		events: make(chan *backend.BigQuerySchema),
	}

	go watch.StartWatcher(ctx, cfg, cfg.FlagBufferSize, cfg.UploadInterval, job.backends, job.events)

	router := mux.NewRouter()
	router.Path("/incoming").Handler(middleware(http.HandlerFunc(event), job)).Methods("POST")

	logrus.Infof("Start web server %s", cfg.FlagWebServerBind)
	return http.ListenAndServe(cfg.FlagWebServerBind, router)
}

// handlers
func event(w http.ResponseWriter, r *http.Request) {
	job := requestJobFromContext(r.Context())
	e := &backend.BigQuerySchema{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(e); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	e.Timestamp = time.Now()
	job.events <- e
}
