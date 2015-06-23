package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestCountReduce(t *testing.T) {

	handler := NewReduceHandler(&ReduceCount{})

	item := []string{"foo", "bar", "fizz", "fizz", "fizz", "fuzz", "buzz", "baz"}

	for _, i := range item {
		handler.ReduceNext(i)
	}

	result := handler.Result()

	assert.Equal(t, uint64(len(item)), result, "Result should equal the size of the item array")
}

func TestCountDistinctReduce(t *testing.T) {

	handler := NewReduceHandler(&ReduceCountDistinct{})

	item := []string{"foo", "foo", "bar", "fizz", "fizz", "fizz", "fuzz", "buzz", "baz", "baz"}

	for _, i := range item {
		handler.ReduceNext(i)
	}

	result := handler.Result()

	assert.Equal(t, 6, result, "Result should equal the size of the distinct items in the item array")
}

func TestMeanAvgReduce(t *testing.T) {

	handler := NewReduceHandler(&ReduceMeanAvg{})

	item := []float64{1.0, 2.5, 5.3, 1, 7.8, 456}

	for _, i := range item {
		handler.ReduceNext(i)
	}

	result := handler.Result()

	assert.InDelta(t, 78.9333333333, result, 0.000001, "Result should equal the mean avergage of the items in the item array")
}
