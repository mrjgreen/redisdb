package main

import (
	"net"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/mrjgreen/redisdb/utils"
)

type HTTPListener struct {
	Store       SeriesStore
	Log         utils.Logger
	BindAddress string
	listener    net.Listener
}

func (self *HTTPListener) WriteCommand(w rest.ResponseWriter, r *rest.Request) {

	series := r.PathParam("series")

	data := DataValue{}

	err := r.DecodeJsonPayload(&data)

	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	self.Store.Insert(series, data)
}

func (self *HTTPListener) DropCommand(w rest.ResponseWriter, r *rest.Request) {

	series := r.PathParam("series")

	err := self.Store.Drop(series)

	w.WriteJson(err)
}

func (self *HTTPListener) DeleteCommand(w rest.ResponseWriter, r *rest.Request) {

	series := r.PathParam("series")

	between := NewRangeFull()

	deleted, err := self.Store.Delete(series, between)

	if err != nil {
		w.WriteJson(err)
	} else {
		w.WriteJson(map[string]int{"deleted": deleted})
	}
}

func (self *HTTPListener) ReadCommand(w rest.ResponseWriter, r *rest.Request) {

	series := r.PathParam("series")

	search := SeriesSearch{
		Between: NewRangeFull(),
	}

	results := self.Store.Search(series, search)

	w.WriteJson(results)
}

// Open starts the service
func (self *HTTPListener) Start() error {

	api := rest.NewApi()

	stack := []rest.Middleware{
		&rest.TimerMiddleware{},
		&rest.RecorderMiddleware{},
		&rest.PoweredByMiddleware{},
		&rest.RecoverMiddleware{},
		&rest.JsonIndentMiddleware{},
		&rest.ContentTypeCheckerMiddleware{},
	}

	api.Use(stack...)

	router, _ := rest.MakeRouter(
		rest.Post("/series/:series/data", self.WriteCommand),
		rest.Get("/series/:series/data", self.ReadCommand),
		rest.Delete("/series/:series/data", self.DeleteCommand),
		rest.Delete("/series/:series", self.DropCommand),
	)

	api.SetApp(router)

	self.Log.Infof("Started HTTP interface on %s", self.BindAddress)

	ln, err := net.Listen("tcp", self.BindAddress)
	if err != nil {
		return err
	}
	self.listener = ln

	http.Serve(self.listener, api.MakeHandler())

	return nil
}

// Close closes the underlying listener.
func (self *HTTPListener) Stop() {
	self.Log.Debugf("Closing HTTP interface on %s", self.BindAddress)

	self.listener.Close()

	self.Log.Infof("Closed HTTP interface on %s", self.BindAddress)
}
