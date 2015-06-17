package simpleflake

import (
	"time"
	"math/rand"
)

type SimpleFlakeResponse struct {
	Time time.Time
	Id uint64
}

func NewId() *SimpleFlakeResponse{

	// Epoch for simpleflake timestamps, starts at the year 2000
	const EPOCH = 946702800000
	const TIMESTAMP_LENGTH = 41
	const RANDOM_MAX = 2 << 23

	var time_now = time.Now()

	var millisecond_time = (time_now.UnixNano() / int64(time.Millisecond)) - EPOCH;

	var randomness = uint64(rand.Int31n(RANDOM_MAX))

	var id = uint64(millisecond_time << TIMESTAMP_LENGTH) + randomness

	return &SimpleFlakeResponse{Time: time_now, Id: id}
}

func SeedNanoTime() {
	rand.Seed(time.Now().UTC().UnixNano())
}
