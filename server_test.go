package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type m map[string]interface{}

type RequestSender struct {
	Host string
}

func (self *RequestSender) sendJsonRequest(path string, method string, data m) ([]m, error) {

	url := self.Host + path
	//fmt.Println("Sending test request:>", url)

	var req *http.Request

	if method == "GET" {
		req, _ = http.NewRequest(method, url, nil)
	} else {
		jsonStr, _ := json.Marshal(data)
		req, _ = http.NewRequest(method, url, bytes.NewBuffer(jsonStr))
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}

	resp, err := client.Do(req)

	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	//fmt.Println("Received: " + string(body))

	var responseMap []m

	json.Unmarshal(body, &responseMap)

	return responseMap, err
}

func getServer(t *testing.T) (*RequestSender, *Server) {

	config := NewConfig()

	s, err := NewServer(config)

	// Clear everything
	s.mgo.DB("data").DropDatabase()
	s.mgo.DB("config").DropDatabase()

	if err != nil {
		t.Fatalf(err.Error())
	}

	if err := s.Start(); err != nil {
		t.Fatalf(err.Error())
	}

	sender := &RequestSender{"http://localhost" + config.HTTP.Port}

	return sender, s
}

func TestServerStarts(t *testing.T) {

	sender, _ := getServer(t)

	data := m{"foo": "bar", "baz": 123}

	// Drop the test series first
	sender.sendJsonRequest("/series/test:series", "DELETE", nil)
	sender.sendJsonRequest("/series/test:filter:series", "DELETE", nil)

	// Post some data
	_, err := sender.sendJsonRequest("/series/test:series/data", "POST", data)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Get the data
	returndata, err := sender.sendJsonRequest("/series/test:series/data", "GET", nil)

	if err != nil {
		t.Fatalf(err.Error())
	}

	require.Len(t, returndata, 1)

	_, ok := returndata[0]["time"]

	assert.True(t, ok, "Response should contain a time key")
	assert.Equal(t, data["foo"], returndata[0]["foo"], "Return data should match the posted data")
	assert.Equal(t, data["baz"].(int), int(returndata[0]["baz"].(float64)), "Return int should match the posted data")

	// List the series
	returndata, err = sender.sendJsonRequest("/series", "GET", nil)

	require.Len(t, returndata, 1)

	assert.Equal(t, "test:series", returndata[0]["name"], "Series name should be test:series")

	// Post some data under a different name so we can test the filter list
	_, err = sender.sendJsonRequest("/series/test:filter:series/data", "POST", data)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// List the series
	returndata, err = sender.sendJsonRequest("/series?filter=test:filter:*", "GET", nil)

	require.Len(t, returndata, 1)

	assert.Equal(t, "test:filter:series", returndata[0]["name"], "Series name should be test:series")
	assert.Equal(t, []interface{}{"series"}, returndata[0]["matches"], "Glob pattern should have matched on series name")

	// Delete the data
	sender.sendJsonRequest("/series/test:series/data", "DELETE", nil)

	// Get the data - should now be empty
	returndata, err = sender.sendJsonRequest("/series/test:series/data", "GET", nil)

	if err != nil {
		t.Fatalf(err.Error())
	}

	assert.Len(t, returndata, 0)

	//s.Stop()
}

func TestContinuousQueries(t *testing.T) {

	sender, _ := getServer(t)

	sender.sendJsonRequest("/query/click:event:10m:c:*", "DELETE", nil)

	data := m{
		"SourceSeries": "click:raw:c:*",
		"TargetSeries": "click:event:10m:c:*", // The glob pattern of source will be mapped onto the target
		"Granularity":  "10m",
		"Query": m{
			"Values": m{
				"count":     m{"$sum": 1},
				"avg_value": m{"$avg": "$value"},
				"sum_value": m{"$sum": "$value"},
			},
			"Group": m{
				"Enabled": true,
				"Columns": m{"event": "$event"},
			},
		},
	}

	sender.sendJsonRequest("/query", "POST", data)

	returndata, err := sender.sendJsonRequest("/query", "GET", nil)

	if err != nil {
		t.Fatalf(err.Error())
	}

	assert.Len(t, returndata, 1)

	sender.sendJsonRequest("/query/click:event:10m:c:*", "DELETE", nil)

	returndata, err = sender.sendJsonRequest("/query", "GET", nil)

	if err != nil {
		t.Fatalf(err.Error())
	}

	assert.Len(t, returndata, 0)
}

func TestRetentionPolicy(t *testing.T) {

	sender, _ := getServer(t)

	sender.sendJsonRequest("/retention/click:event:10m:c:*", "DELETE", nil)

	data := m{
		"name": "click:raw:c:*",
		"time": "10m",
	}

	sender.sendJsonRequest("/retention", "POST", data)

	returndata, err := sender.sendJsonRequest("/retention", "GET", nil)

	if err != nil {
		t.Fatalf(err.Error())
	}

	assert.Len(t, returndata, 1)

	sender.sendJsonRequest("/retention/click:event:10m:c:*", "DELETE", nil)

	returndata, err = sender.sendJsonRequest("/retention", "GET", nil)

	if err != nil {
		t.Fatalf(err.Error())
	}
}
