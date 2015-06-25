package main

import (
	"fmt"
	"net/http"
	//"encoding/json"
	"github.com/ant0ine/go-json-rest/rest"
	log "gopkg.in/inconshreveable/log15.v2"
)

type HttpInterface struct{
	Store SeriesStore
	Log log.Logger
	BindAddress string
}

func (self *HttpInterface) WriteCommand(w rest.ResponseWriter, r *rest.Request){

	series := r.PathParam("series")

	data := &SeriesData{}

	err := r.DecodeJsonPayload(data)

	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	self.Store.Insert(series, data)

	w.WriteJson(data)
}

func (self *HttpInterface) ReadCommand(w rest.ResponseWriter, r *rest.Request){

	series := r.PathParam("series")

	search := SeriesSearch{
		Between : NewRangeFull(),
	}
//
//	err := r.DecodeJsonPayload(&search)
//
//	if err != nil {
//		rest.Error(w, err.Error(), http.StatusInternalServerError)
//		return
//	}

	results := self.Store.Search(series, search)

	w.WriteJson(results)
}

// Open starts the service
func (self *HttpInterface) Start() error {

	api := rest.NewApi()

	api.Use(rest.DefaultDevStack...)

	router, _ := rest.MakeRouter(
		rest.Post("/write/:series", self.WriteCommand),
		rest.Get("/read/:series", self.ReadCommand),
//		rest.Get("/status", self.StatusCommand),
//		rest.Put("/admin", self.AdminCommand),
	)

	api.SetApp(router)

	self.Log.Info(fmt.Sprintf("Started HTTP interface on %s", self.BindAddress))

	// Begin listening for requests in a separate goroutine.
	http.ListenAndServe(self.BindAddress, api.MakeHandler())

	return nil
}

// Close closes the underlying listener.
func (self *HttpInterface) Close() error {

	return nil
}
