package main

import (
	"net"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/mrjgreen/redisdb/utils"
)

type HTTPListener struct {
	Store                  SeriesStore
	RetentionPolicyManager *RetentionPolicyManager
	ContinuousQueryManager *ContinuousQueryManager
	Log                    utils.Logger
	BindAddress            string
	listener               net.Listener
}

type intResult map[string]int

func (self *HTTPListener) WriteCommand(w rest.ResponseWriter, r *rest.Request) {

	data := DataValue{}

	err := r.DecodeJsonPayload(&data)

	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = self.Store.Insert(r.PathParam("series"), data)

	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.WriteJson(intResult{"inserted": 1})
}

func (self *HTTPListener) DropCommand(w rest.ResponseWriter, r *rest.Request) {

	err := self.Store.Drop(r.PathParam("series"))

	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.WriteJson(intResult{"dropped": 1})
}

func (self *HTTPListener) DeleteCommand(w rest.ResponseWriter, r *rest.Request) {

	between := NewRangeFull()

	deleted, err := self.Store.Delete(r.PathParam("series"), between)

	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteJson(intResult{"deleted": deleted})
}

func (self *HTTPListener) ReadCommand(w rest.ResponseWriter, r *rest.Request) {

	search := SeriesSearch{
		Between: NewRangeFull(),
	}

	results := self.Store.Search(r.PathParam("series"), search)

	w.WriteJson(results)
}

func (self *HTTPListener) ListSeries(w rest.ResponseWriter, r *rest.Request) {

	results := self.Store.List(r.URL.Query().Get("filter"))

	w.WriteJson(results)
}

func (self *HTTPListener) ListContinuousQueries(w rest.ResponseWriter, r *rest.Request) {

	results := self.ContinuousQueryManager.List() //TODO - r.URL.Query().Get("filter")

	w.WriteJson(results)
}

func (self *HTTPListener) DeleteContinuousQuery(w rest.ResponseWriter, r *rest.Request) {

	self.ContinuousQueryManager.Delete(r.PathParam("query_name"))

	w.WriteJson(intResult{"deleted": 1})
}

func (self *HTTPListener) AddContinuousQuery(w rest.ResponseWriter, r *rest.Request) {

	data := ContinuousQuery{}

	err := r.DecodeJsonPayload(&data)

	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	self.ContinuousQueryManager.Add(data)

	w.WriteJson(intResult{"inserted": 1})
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
		rest.Get("/series", self.ListSeries),
		rest.Delete("/series/:series", self.DropCommand),

		rest.Get("/query", self.ListContinuousQueries),
		rest.Post("/query", self.AddContinuousQuery),
		//rest.Put("/query/:query_name", self.UpdateContinuousQuery),
		rest.Delete("/query/:query_name", self.DeleteContinuousQuery),
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
