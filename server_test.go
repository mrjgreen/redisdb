package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	fmt.Println("Sending test request:>", url)

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

	fmt.Println("Received: " + string(body))

	var responseMap []m

	json.Unmarshal(body, &responseMap)

	return responseMap, err
}

func TestServerStarts(t *testing.T) {

	config := NewConfig()

	s, err := NewServer(config)

	if err != nil {
		t.Fatalf(err.Error())
	}

	if err := s.Start(); err != nil {
		t.Fatalf(err.Error())
	}

	data := m{"foo": "bar", "baz": 123}

	sender := &RequestSender{"http://localhost" + config.HTTP.Port}

	// Drop the test series first
	sender.sendJsonRequest("/series/test:series", "DELETE", nil)

	// Post some data
	_, err = sender.sendJsonRequest("/series/test:series/data", "POST", data)

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

	// Delete the data
	sender.sendJsonRequest("/series/test:series/data", "DELETE", nil)

	// Get the data - should now be empty
	returndata, err = sender.sendJsonRequest("/series/test:series/data", "GET", nil)

	if err != nil {
		t.Fatalf(err.Error())
	}

	assert.Len(t, returndata, 0)
}

//
// // Server represents a container for the metadata and storage data and services.
// // It is built using a Config and it manages the startup and shutdown of all
// // services in the proper order.
// type BenchMark struct {
// 	Log                    utils.Logger
// 	Store                  SeriesStore
// 	RetentionPolicyManager *RetentionPolicyManager
// 	ContinuousQueryManager *ContinuousQueryManager
// }
//
// // Start will intialize the tests
// func (s *BenchMark) Start() error {
//
// 	s.Log.Infof("Running test inserts")
//
// 	s.RetentionPolicyManager.Delete("click:raw:c:*")
//
// 	s.RetentionPolicyManager.Add(RetentionPolicy{"click:raw:c:*", time.Duration(120 * time.Second)})
//
// 	s.ContinuousQueryManager.Delete("click:1m:c:*")
//
// 	s.ContinuousQueryManager.Add(ContinuousQuery{
// 		SourceSeries: "click:raw:c:*",
// 		TargetSeries: "click:1m:c:*", // The glob pattern of source will be mapped onto the target
// 		Granularity:  "1m",
// 		Query: SeriesSearch{
// 			Values: SearchValues{
// 				"count": SearchValue{"$sum": 1},
// 			},
// 			Group: SearchGroupBy{Enabled: true},
// 		},
// 	})
//
// 	s.ContinuousQueryManager.Delete("click:event:10m:c:*")
//
// 	s.ContinuousQueryManager.Add(ContinuousQuery{
// 		SourceSeries: "click:raw:c:*",
// 		TargetSeries: "click:event:10m:c:*", // The glob pattern of source will be mapped onto the target
// 		Granularity:  "10m",
// 		Query: SeriesSearch{
// 			Values: SearchValues{
// 				"count":     SearchValue{"$sum": 1},
// 				"avg_value": SearchValue{"$avg": "$value"},
// 				"sum_value": SearchValue{"$sum": "$value"},
// 			},
// 			Group: SearchGroupBy{
// 				Enabled: true,
// 				Columns: GroupColumn{"event": "$event"},
// 			},
// 		},
// 	})
//
// 	// var i = 0
//
// 	// for {
//
// 	// 	i++
// 	// 	campaignTag := "12345"
//
// 	// 	point := DataValue{
// 	// 		"value": i,
// 	// 		"event": i % 5,
// 	// 	}
//
// 	// 	s.Store.Insert("click:raw:c:"+campaignTag, point)
//
// 	// 	time.Sleep(100 * time.Nanosecond)
// 	// }
//
// 	return nil
// }
