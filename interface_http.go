package main

import (
	"net"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/mrjgreen/redisdb/utils"
)

type HttpInterface struct {
	Store       SeriesStore
	Log         utils.Logger
	BindAddress string
	listener    net.Listener
}

func (self *HttpInterface) WriteCommand(w rest.ResponseWriter, r *rest.Request) {

	series := r.PathParam("series")

	data := DataValue{}

	err := r.DecodeJsonPayload(&data)

	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	self.Store.Insert(series, data)
}

func (self *HttpInterface) ReadCommand(w rest.ResponseWriter, r *rest.Request) {

	series := r.PathParam("series")

	search := SeriesSearch{
		Between: NewRangeFull(),
	}

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
	)

	api.SetApp(router)

	self.Log.Infof("Started HTTP interface on %s", self.BindAddress)

	ln, err := net.Listen("tcp", self.BindAddress)
	if err != nil {
		return err
	}
	self.listener = ln

	// Begin listening for requests in a separate goroutine.
	http.Serve(self.listener, api.MakeHandler())

	return nil
}

// Close closes the underlying listener.
func (self *HttpInterface) Stop() {
	self.Log.Debugf("Closing HTTP interface on %s", self.BindAddress)

	self.listener.Close()

	self.Log.Infof("Closed HTTP interface on %s", self.BindAddress)
}
