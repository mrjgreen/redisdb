package main

import (
	"fmt"
	"net/http"
	"github.com/ant0ine/go-json-rest/rest"
	log "gopkg.in/inconshreveable/log15.v2"
)

type HttpInterface struct{
	Store SeriesStore
	Log log.Logger
	BindAddress string
}

func (self *HttpInterface) WriteCommand(w rest.ResponseWriter, req *rest.Request){

}

func (self *HttpInterface) ReadCommand(w rest.ResponseWriter, req *rest.Request){

}

// Open starts the service
func (self *HttpInterface) Start() error {

	api := rest.NewApi()

	api.Use(rest.DefaultDevStack...)

	router, _ := rest.MakeRouter(
		rest.Post("/write", self.WriteCommand),
		rest.Get("/read", self.ReadCommand),
//		rest.Get("/status", self.StatusCommand),
//		rest.Put("/admin", self.AdminCommand),
	)

	api.SetApp(router)

	// Begin listening for requests in a separate goroutine.
	go func(){
		http.ListenAndServe(self.BindAddress, api.MakeHandler())
	}()

	self.Log.Info(fmt.Sprintf("Started HTTP interface on %s", self.BindAddress))

	return nil
}

// Close closes the underlying listener.
func (self *HttpInterface) Close() error {

	return nil
}
