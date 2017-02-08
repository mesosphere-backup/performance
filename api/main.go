package main

import (
	"encoding/json"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

func main() {
	router := mux.NewRouter()
	router.Path("/incoming").Methods("POST").HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			decoder := json.NewDecoder(r.Body)
			var i interface{}
			if err := decoder.Decode(&i); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			logrus.Infof("%+v", i)
		})

	logrus.Infof("Start web server 9123", )
	if err := http.ListenAndServe(":9123", router); err != nil {
		panic(err)
	}
}


