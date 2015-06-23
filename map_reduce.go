package main

import ()

type ReduceFuncIterator struct{
	previousValue interface{}
	count uint64
	handler ReduceFunc
}

type ReduceFunc interface{
	Reduce(previous interface{}, current interface{}, count uint64) interface{}
	Result(previous interface{}, count uint64) interface{}
}


func (self *ReduceFuncIterator) ReduceNext(item interface{}){

	self.count++

	self.previousValue = self.handler.Reduce(self.previousValue, item, self.count)
}

func (self *ReduceFuncIterator) Result() interface{}{
	return self.handler.Result(self.previousValue, self.count)
}

func NewReduceHandler(reduce ReduceFunc) ReduceFuncIterator{
	return ReduceFuncIterator{
		handler :reduce,
	}
}



//
// Counts a stream of items
//
type ReduceCount struct{}

func (self ReduceCount) Reduce(previous interface{}, current interface{}, count uint64) interface{}{

	return nil;
}

func (self ReduceCount) Result(previous interface{}, count uint64) interface{}{
	return count
}

//
// Counts the distinct items in a stream
//
type ReduceCountDistinct struct{}

type distinctMap map[interface{}]bool

func (self ReduceCountDistinct) Reduce(previous interface{}, current interface{}, count uint64) interface{}{

	if previous == nil{
		previous = make(distinctMap)
	}

	previous.(distinctMap)[current] = true

	return previous
}

func (self ReduceCountDistinct) Result(previous interface{}, count uint64) interface{}{
	return len(previous.(distinctMap))
}


//
// Calculates the mean average of a value in a stream
//
type ReduceMeanAvg struct{}

func (self ReduceMeanAvg) Reduce(previous interface{}, current interface{}, count uint64) interface{}{

	if previous == nil{
		previous = float64(0)
	}

	return previous.(float64) + current.(float64)
}

func (self ReduceMeanAvg) Result(previous interface{}, count uint64) interface{}{

	return previous.(float64) / float64(count)
}


//
// Calculates the sum of a value in a stream
//
type ReduceSum struct{}

func (self ReduceSum) Reduce(previous interface{}, current interface{}, count uint64) interface{}{

	if previous == nil{
		previous = float64(0)
	}

	return previous.(float64) + current.(float64)
}

func (self ReduceSum) Result(previous interface{}, count uint64) interface{}{

	return previous.(float64)
}
